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

#include <Aggregation/Function/CountDistinctAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

CountDistinctAggregationPhysicalFunction::CountDistinctAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRefPagedVector)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , bufferRefPagedVector(std::move(bufferRefPagedVector))
{
}

void CountDistinctAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// Adding the record to the paged vector. We are storing the full record in the paged vector for now.
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(memArea, bufferRefPagedVector);
    pagedVectorRef.writeRecord(record, pipelineMemoryProvider.bufferProvider);
}

void CountDistinctAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    /// Getting the paged vectors from the aggregation states
    const auto memArea1 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState2);

    /// Calling the copyFrom function of the paged vector to combine the two paged vectors by copying the content of the second paged vector to the first paged vector
    nautilus::invoke(
        +[](Interface::PagedVector* vector1, const Interface::PagedVector* vector2) -> void { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record CountDistinctAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Getting the paged vector from the aggregation state
    const auto pagedVectorPtr = static_cast<nautilus::val<Interface::PagedVector*>>(aggregationState);
    const Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRefPagedVector);
    const auto allFieldNames = bufferRefPagedVector->getMemoryLayout()->getSchema().getFieldNames();

    /// The number of distinct values the state holds: a value counts once, at the first position that holds it. The scan
    /// is quadratic, as the median's is.
    nautilus::val<uint64_t> distinct = 0;
    nautilus::val<uint64_t> candidatePos = 0;
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto candidateValue = inputFunction.execute(*candidateIt, pipelineMemoryProvider.arena);
        nautilus::val<bool> seenBefore(false);
        nautilus::val<uint64_t> itemPos = 0;
        for (auto itemIt = pagedVectorRef.begin(allFieldNames); itemIt != endIt; ++itemIt)
        {
            if (itemPos < candidatePos)
            {
                const auto itemValue = inputFunction.execute(*itemIt, pipelineMemoryProvider.arena);
                if (itemValue == candidateValue)
                {
                    seenBefore = true;
                }
            }
            itemPos = itemPos + 1;
        }
        if (not seenBefore)
        {
            distinct = distinct + 1;
        }
        candidatePos = candidatePos + 1;
    }

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal(distinct).castToType(resultType.type));
    return resultRecord;
}

void CountDistinctAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Allocates a new PagedVector in the memory area provided by the pointer to the pagedvector
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

void CountDistinctAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            /// Calls the destructor of the PagedVector
            auto* pagedVector
                = reinterpret_cast<Interface::PagedVector*>(pagedVectorMemArea); /// NOLINT(cppcoreguidelines-pro-type-reinterpret-cast)
            pagedVector->~PagedVector();
        },
        aggregationState);
}

size_t CountDistinctAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Interface::PagedVector);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterCountDistinctAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.bufferRefPagedVector.has_value(), "Memory provider paged vector not set");
    return std::make_shared<CountDistinctAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.bufferRefPagedVector.value());
}

}
