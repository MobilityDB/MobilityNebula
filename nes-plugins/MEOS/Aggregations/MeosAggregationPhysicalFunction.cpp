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

#include <Aggregations/MeosAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <utility>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregations/MeosAggregationLogicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/ExtensionType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Util/Strings.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <MeosAggregateRuntime.hpp>
#include <MeosKernelRuntime.hpp>
#include <function.hpp>
#include <val.hpp>

namespace NES
{

namespace
{
/// The argument naming a variable-sized value, written in its slot, as a MeosPhysicalFunction writes it
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

/// Whether the aggregate answered no value, as its Finish flags it
bool answeredNull(const int8_t* out)
{
    return out[MeosRuntime::AnswerNullAt] != 0;
}

/// The value at the end of the block
template <typename T>
void append(std::vector<int8_t>& block, const T value)
{
    const auto at = block.size();
    block.resize(at + sizeof(T));
    std::memcpy(block.data() + at, &value, sizeof(T));
}

/// The text of a constant, without the quotes of a string
std::string constantText(const ConstantValueLogicalFunction& constant)
{
    auto text = constant.getConstantValue();
    if (text.size() >= 2 and text.front() == '\'' and text.back() == '\'')
    {
        text = text.substr(1, text.size() - 2);
    }
    return text;
}

/// The block of the constants: the offset of each, then each in the form a MEOS kernel takes an argument of its
/// type, a variable-sized one as the Argument naming its bytes, a number in a slot of 8 bytes. A typed constant
/// of a MEOS type (TBOX('...')) holds the memory image its type reads from the text, an untyped one its text.
/// The logical aggregation chose the overload whose literal types the constants read as.
std::shared_ptr<MeosAggregationPhysicalFunction::LiteralBlock>
literalBlock(const std::span<const DataType::Type> types, const std::vector<LogicalFunction>& literals)
{
    PRECONDITION(types.size() == literals.size(), "an overload of a MEOS aggregate takes one constant per literal type it states");
    auto result = std::make_shared<MeosAggregationPhysicalFunction::LiteralBlock>();
    std::vector<std::string> texts;
    for (size_t i = 0; i < literals.size(); ++i)
    {
        const auto constant = literals[i].tryGetAs<ConstantValueLogicalFunction>();
        PRECONDITION(constant.has_value(), "a constant of a MEOS aggregate is a constant value");
        const auto text = constantText(constant->get());
        texts.push_back(text);
        std::vector<int8_t> value;
        if (types[i] == DataType::Type::VARSIZED)
        {
            const auto type = (*constant)->getDataType();
            if (type.extension != nullptr and type.extension->fromText != nullptr)
            {
                const auto bytes = type.extension->fromText(text);
                value.resize(bytes.size());
                std::memcpy(value.data(), bytes.data(), bytes.size());
            }
            else
            {
                value.assign(text.begin(), text.end());
            }
        }
        result->values.push_back(std::move(value));
    }
    /// The values do not move once the block names them
    auto& block = result->block;
    block.resize(literals.size() * sizeof(uint32_t));
    for (size_t i = 0; i < literals.size(); ++i)
    {
        const auto offset = static_cast<uint32_t>(block.size());
        std::memcpy(block.data() + (i * sizeof(uint32_t)), &offset, sizeof(offset));
        const auto& text = texts[i];
        switch (types[i])
        {
            case DataType::Type::VARSIZED:
                append(
                    block,
                    MeosRuntime::Argument{.size = result->values[i].size(), .content = result->values[i].data()});
                break;
            case DataType::Type::BOOLEAN:
                append(block, static_cast<int64_t>(toUpperCase(text) == "TRUE"));
                break;
            case DataType::Type::FLOAT64:
                append(block, std::stod(text));
                break;
            case DataType::Type::UINT64:
                append(block, static_cast<uint64_t>(std::stoull(text)));
                break;
            default:
                append(block, static_cast<int64_t>(std::stoll(text)));
                break;
        }
    }
    return result;
}
}

MeosAggregationPhysicalFunction::MeosAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    const DataType::Type argumentType,
    const std::span<const DataType::Type> literalTypes,
    const std::vector<LogicalFunction>& literals,
    const MeosAggregateKernels& kernels)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , argumentType(argumentType)
    , literals(literalBlock(literalTypes, literals))
    , kernels(kernels)
{
}

void MeosAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto slot = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto block = nautilus::val<int8_t*>(literals->block.data());
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto update = [&]
    {
        auto argument = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(sizeof(MeosRuntime::Argument)));
        if (argumentType == DataType::Type::VARSIZED)
        {
            const auto data = value.getRawValueAs<VariableSizedData>();
            nautilus::invoke(nameArgument, argument, data.getSize(), data.getContent());
        }
        else
        {
            value.castToType(argumentType).writeToMemory(argument);
        }
        nautilus::invoke(kernels.update, slot, argument, block);
    };
    if (value.isNullable())
    {
        /// The aggregate is STRICT: a null value is skipped
        if (not value.isNull())
        {
            update();
        }
        return;
    }
    update();
}

void MeosAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        kernels.combine, static_cast<nautilus::val<int8_t*>>(aggregationState1), static_cast<nautilus::val<int8_t*>>(aggregationState2));
}

Record MeosAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto out = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(MeosRuntime::AnswerSize));
    const auto answer = nautilus::invoke(kernels.finish, static_cast<nautilus::val<int8_t*>>(aggregationState), out);
    const auto null = nautilus::invoke(answeredNull, out);
    Record record;
    if (resultType.type != DataType::Type::VARSIZED)
    {
        record.write(resultFieldIdentifier, VarVal::readVarValFromMemory(out, DataType{resultType.type, DataType::NULLABLE::IS_NULLABLE}, null));
        return record;
    }
    const auto length = nautilus::invoke(answeredLength, answer);
    auto result = pipelineMemoryProvider.arena.allocateVariableSizedData(length);
    nautilus::invoke(keepAnswer, result.getContent(), answer, length);
    record.write(resultFieldIdentifier, VarVal{result, true, null});
    return record;
}

void MeosAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider&)
{
    nautilus::invoke(kernels.initialize, static_cast<nautilus::val<int8_t*>>(aggregationState));
}

void MeosAggregationPhysicalFunction::cleanup(const nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(kernels.destroy, static_cast<nautilus::val<int8_t*>>(aggregationState));
}

size_t MeosAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return kernels.stateSize;
}

std::shared_ptr<AggregationPhysicalFunction> meosAggregation(
    AggregationPhysicalFunctionRegistryArguments arguments,
    const DataType::Type argumentType,
    const std::span<const DataType::Type> literalTypes,
    const MeosAggregateKernels& kernels)
{
    std::vector<LogicalFunction> literals;
    if (arguments.logicalFunction)
    {
        if (const auto logical = arguments.logicalFunction->tryGetAs<MeosAggregationLogicalFunction>())
        {
            literals = logical->get().getLiterals();
        }
    }
    return std::make_shared<MeosAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        std::move(arguments.inputFunction),
        std::move(arguments.resultFieldIdentifier),
        argumentType,
        literalTypes,
        literals,
        kernels);
}

}
