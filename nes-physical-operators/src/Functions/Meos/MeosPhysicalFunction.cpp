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

#include <Functions/Meos/MeosPhysicalFunction.hpp>

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
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

/// The kernel called on the argument pointers, all of them, in order
template <typename Kernel, size_t... Index>
nautilus::val<int8_t*> callKernel(
    Kernel kernel, const std::vector<nautilus::val<int8_t*>>& pointers, const nautilus::val<int8_t*>& out, std::index_sequence<Index...>)
{
    return nautilus::invoke(kernel, pointers[Index]..., out);
}

/// The length of the value a kernel answers, 0 for none
uint32_t answeredLength(const int8_t* answer)
{
    uint32_t length = 0;
    if (answer != nullptr)
    {
        std::memcpy(&length, answer, sizeof(length));
    }
    return length;
}

/// The bytes of the value a kernel answers copied where they are kept, and the answer freed
void keepAnswer(int8_t* destination, int8_t* answer, const uint32_t length)
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
    std::vector<nautilus::val<int8_t*>> pointers;
    pointers.reserve(arguments.size());
    for (size_t i = 0; i < arguments.size(); ++i)
    {
        const auto value = arguments[i].execute(record, arena);
        if (argumentTypes[i] == DataType::Type::VARSIZED)
        {
            pointers.emplace_back(value.cast<VariableSizedData>().getReference());
        }
        else
        {
            auto slot = arena.allocateMemory(nautilus::val<size_t>(sizeof(int64_t)));
            value.castToType(argumentTypes[i]).writeToMemory(slot);
            pointers.emplace_back(slot);
        }
    }
    const auto out = arena.allocateMemory(nautilus::val<size_t>(sizeof(int64_t)));
    const auto answer = std::visit(
        [&]<typename Kernel>(Kernel typedKernel)
        { return callKernel(typedKernel, pointers, out, std::make_index_sequence<KernelArity<Kernel>::value>{}); },
        kernel);
    if (resultType != DataType::Type::VARSIZED)
    {
        return VarVal::readVarValFromMemory(out, resultType);
    }
    const auto length = nautilus::invoke(answeredLength, answer);
    auto result = arena.allocateVariableSizedData(length);
    nautilus::invoke(keepAnswer, result.getContent(), answer, length);
    VarVal(length).writeToMemory(result.getReference());
    return result;
}

}
