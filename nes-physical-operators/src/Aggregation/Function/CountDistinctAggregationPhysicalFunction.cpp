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
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <nautilus/function.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// The paged vector of the window, a child of the hash map's buffer whose index the state holds
OwnedNautilusBuffer loadPagedVector(const nautilus::val<AggregationState*>& aggregationState, BorrowedNautilusBuffer parentBuffer)
{
    OwnedNautilusBuffer pagedVecBuffer;
    nautilus::invoke(
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr) { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVecBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState));
    return pagedVecBuffer;
}
}

CountDistinctAggregationPhysicalFunction::CountDistinctAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void CountDistinctAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// SQL-standard: a null is no value, so COUNT(DISTINCT x) does not count it
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (not value.isNull())
    {
        auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void CountDistinctAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer parentBuffer1,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer parentBuffer2,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    /// Copying the pages of the second paged vector into the first, as the median combines its records
    nautilus::invoke(
        +[](AbstractBufferProvider* bufferProvider,
            TupleBuffer* parent1,
            const uint32_t* indexPtr1,
            TupleBuffer* parent2,
            const uint32_t* indexPtr2) -> void
        {
            const TupleBuffer vec1Buf = parent1->loadChildBuffer(ChildBufferIndex{*indexPtr1});
            const TupleBuffer vec2Buf = parent2->loadChildBuffer(ChildBufferIndex{*indexPtr2});
            auto vector1 = PagedVector::load(vec1Buf);
            const auto vector2 = PagedVector::load(vec2Buf);
            vector1.copyPagesFrom(*bufferProvider, vector2);
        },
        pipelineMemoryProvider.bufferProvider,
        parentBuffer1.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState1),
        parentBuffer2.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState2));
}

Record CountDistinctAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
    const auto pagedVectorEnd = pagedVectorRef.end();

    /// A value is counted at the first position that holds it: a candidate counts when no record before it holds the same value
    nautilus::val<uint64_t> distinct = 0;
    nautilus::val<uint64_t> candidatePos = 0;
    for (auto candidateIt = pagedVectorRef.begin(); candidateIt != pagedVectorEnd; ++candidateIt)
    {
        const auto candidateValue = inputFunction.execute(*candidateIt, pipelineMemoryProvider.arena);
        nautilus::val<bool> seenBefore{false};
        nautilus::val<uint64_t> itemPos = 0;
        for (auto itemIt = pagedVectorRef.begin(); itemIt != pagedVectorEnd and itemPos < candidatePos; ++itemIt)
        {
            const auto itemValue = inputFunction.execute(*itemIt, pipelineMemoryProvider.arena);
            if (itemValue == candidateValue)
            {
                seenBefore = true;
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
    resultRecord.write(resultFieldIdentifier, VarVal{distinct});
    return resultRecord;
}

void CountDistinctAggregationPhysicalFunction::reset(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    const nautilus::val<uint64_t> tupleSize = getSizeInBytes(tupleLayout->getSchema());
    const nautilus::val<uint32_t> childBufferIndexVal = nautilus::invoke(
        +[](TupleBuffer* parentBuffer, AbstractBufferProvider* bufferProvider, uint64_t tupleSize)
        {
            if (auto pagedVectorBufferOpt = bufferProvider->getUnpooledBuffer(PagedVector::getMainBufferSize()))
            {
                auto pagedVectorBuffer = pagedVectorBufferOpt.value();
                PagedVector::init(pagedVectorBuffer, bufferProvider->getBufferSize(), tupleSize);
                auto childBufferIndex = parentBuffer->storeChildBuffer(pagedVectorBuffer);
                return childBufferIndex.getRawValue();
            }
            throw BufferAllocationFailure("No unpooled TupleBuffer available for count distinct aggregation paged vector!");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(aggregationState) = childBufferIndexVal;
}

void CountDistinctAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the paged vector buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t CountDistinctAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// uint32_t child buffer index (4B); the answer is never null, so the state holds no null flag
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
CountDistinctAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<CountDistinctAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
