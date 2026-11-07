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

#include <Aggregation/Function/Meos/MeosAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <val.hpp>
#include <val_concepts.hpp>

namespace NES
{

namespace
{
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

MeosAggregationPhysicalFunction::MeosAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    const DataType::Type argumentType,
    const MeosAggregateKernels& kernels)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , argumentType(argumentType)
    , kernels(kernels)
{
}

void MeosAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Nautilus::Record& record)
{
    const auto slot = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (argumentType == DataType::Type::VARSIZED)
    {
        nautilus::invoke(kernels.update, slot, value.cast<VariableSizedData>().getReference());
        return;
    }
    auto argument = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(sizeof(int64_t)));
    value.castToType(argumentType).writeToMemory(argument);
    nautilus::invoke(kernels.update, slot, argument);
}

void MeosAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        kernels.combine, static_cast<nautilus::val<int8_t*>>(aggregationState1), static_cast<nautilus::val<int8_t*>>(aggregationState2));
}

Nautilus::Record
MeosAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    const auto out = pipelineMemoryProvider.arena.allocateMemory(nautilus::val<size_t>(sizeof(int64_t)));
    const auto answer = nautilus::invoke(kernels.finish, static_cast<nautilus::val<int8_t*>>(aggregationState), out);
    Nautilus::Record record;
    if (resultType.type != DataType::Type::VARSIZED)
    {
        record.write(resultFieldIdentifier, VarVal::readVarValFromMemory(out, resultType.type));
        return record;
    }
    const auto length = nautilus::invoke(answeredLength, answer);
    auto result = pipelineMemoryProvider.arena.allocateVariableSizedData(length);
    nautilus::invoke(keepAnswer, result.getContent(), answer, length);
    VarVal(length).writeToMemory(result.getReference());
    record.write(resultFieldIdentifier, result);
    return record;
}

void MeosAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
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

}
