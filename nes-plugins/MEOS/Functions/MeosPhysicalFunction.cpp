/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <Functions/MeosPhysicalFunction.hpp>

#include <bit>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <MeosKernelRuntime.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

namespace
{
/// The number of arguments a kernel takes, its last parameter being the slot it answers a number in
template <typename Kernel>
struct KernelArity;

template <typename Result, typename... Parameters>
struct KernelArity<Result (*)(Parameters...)>
{
    static constexpr size_t value = sizeof...(Parameters) - 1;
};

/// The kernel of the given address called on the argument pointers unless an argument is null, when it
/// answers no value. Tested here rather than in the traced code, a null argument adds no branch to the
/// trace, so the trace of an expression grows with the number of its MEOS calls and not as a power of it.
template <typename Indices>
struct GuardedKernel;

template <size_t... Index>
struct GuardedKernel<std::index_sequence<Index...>>
{
    static int8_t* call(const bool null, const uint64_t kernel, MeosKernelArgument<Index>... arguments, int8_t* out)
    {
        if (null)
        {
            return nullptr;
        }
        return std::bit_cast<MeosKernelOfArity<sizeof...(Index)>>(kernel)(arguments..., out);
    }
};

/// The kernel called on the argument pointers, all of them, in order, unless an argument is null
template <typename Kernel, size_t... Index>
nautilus::val<int8_t*> callKernel(
    Kernel kernel,
    const nautilus::val<bool>& null,
    const std::vector<nautilus::val<int8_t*>>& pointers,
    const nautilus::val<int8_t*>& out,
    std::index_sequence<Index...> indices)
{
    return nautilus::invoke(
        &GuardedKernel<decltype(indices)>::call, null, nautilus::val<uint64_t>(std::bit_cast<uint64_t>(kernel)), pointers[Index]..., out);
}

/// The argument naming a variable-sized value, written in its slot
void nameArgument(int8_t* slot, const uint64_t size, const int8_t* content)
{
    const MeosRuntime::Argument argument{.size = size, .content = content};
    std::memcpy(slot, &argument, sizeof(argument));
}

/// The length of the value a kernel answers, 0 for none
uint64_t answeredLength(const int8_t* answer)
{
    uint32_t length = 0;
    if (answer != nullptr)
    {
        std::memcpy(&length, answer, sizeof(length));
    }
    return length;
}

/// The bytes of the value a kernel answers copied where they are kept, and the answer freed
void keepAnswer(int8_t* destination, int8_t* answer, const uint64_t length)
{
    if (answer != nullptr)
    {
        std::memcpy(destination, answer + sizeof(uint32_t), length);
        std::free(answer);
    }
}
}

MeosPhysicalFunction::MeosPhysicalFunction(
    MeosKernel kernel, std::vector<DataType::Type> argumentTypes, const DataType::Type resultType, std::vector<PhysicalFunction> arguments)
    : kernel(kernel), argumentTypes(std::move(argumentTypes)), resultType(resultType), arguments(std::move(arguments))
{
    PRECONDITION(this->argumentTypes.size() == this->arguments.size(), "a MEOS function takes one argument per type of its signature");
}

VarVal MeosPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> values;
    values.reserve(arguments.size());
    bool nullable = false;
    nautilus::val<bool> null = false;
    for (const auto& argument : arguments)
    {
        auto value = argument.execute(record, arena);
        if (value.isNullable())
        {
            nullable = true;
            null = null or value.isNull();
        }
        values.push_back(std::move(value));
    }
    const auto isNullable = nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE;

    std::vector<nautilus::val<int8_t*>> pointers;
    pointers.reserve(values.size());
    for (size_t i = 0; i < values.size(); ++i)
    {
        auto slot = arena.allocateMemory(nautilus::val<size_t>(sizeof(MeosRuntime::Argument)));
        if (argumentTypes[i] == DataType::Type::VARSIZED)
        {
            const auto data = values[i].getRawValueAs<VariableSizedData>();
            nautilus::invoke(nameArgument, slot, data.getSize(), data.getContent());
        }
        else
        {
            values[i].castToType(argumentTypes[i]).writeToMemory(slot);
        }
        pointers.emplace_back(slot);
    }
    const auto out = arena.allocateMemory(nautilus::val<size_t>(sizeof(int64_t)));
    const auto answer = std::visit(
        [&]<typename Kernel>(Kernel typedKernel)
        { return callKernel(typedKernel, null, pointers, out, std::make_index_sequence<KernelArity<Kernel>::value>{}); },
        kernel);
    if (resultType != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(out, DataType{resultType, isNullable}, null);
    }
    const auto length = nautilus::invoke(answeredLength, answer);
    auto result = arena.allocateVariableSizedData(length);
    nautilus::invoke(keepAnswer, result.getContent(), answer, length);
    return VarVal{result, nullable, null};
}

}
