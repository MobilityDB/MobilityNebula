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

#include <Aggregation/Function/ArrayAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundSchema.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/PagedVector/PagedVector.hpp>
#include <Interface/PagedVector/PagedVectorRef.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/function.hpp>
#include <nautilus/std/cstring.h>
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
        +[](TupleBuffer* parent, TupleBuffer* out, const uint32_t* indexPtr)
        { *out = parent->loadChildBuffer(ChildBufferIndex{*indexPtr}); },
        parentBuffer.asArg(),
        pagedVecBuffer.asArg(),
        static_cast<nautilus::val<uint32_t*>>(aggregationState));
    return pagedVecBuffer;
}
}

ArrayAggregationPhysicalFunction::ArrayAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction inputFunction,
    Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<PagedVectorTupleLayout> tupleLayout)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
    , tupleLayout(std::move(tupleLayout))
{
}

void ArrayAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    /// A null has no bytes to hold, so the array leaves it out
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    if (not value.isNull())
    {
        auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
        PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);
        pagedVectorRef.pushBack(record, pipelineMemoryProvider.bufferProvider);
    }
}

void ArrayAggregationPhysicalFunction::combine(
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

Record ArrayAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState,
    BorrowedNautilusBuffer parentBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider)
{
    auto pagedVecBuffer = loadPagedVector(aggregationState, parentBuffer);
    const PagedVectorRef pagedVectorRef(BorrowedNautilusBuffer::from(pagedVecBuffer.asArg()), tupleLayout);

    /// The array is the number of its elements, then each element after its length, as an array type carries it
    const nautilus::val<uint64_t> lengthSize = sizeof(uint32_t);
    nautilus::val<uint64_t> size = lengthSize;
    nautilus::val<uint32_t> count = 0;
    for (const auto& itemRecord : pagedVectorRef)
    {
        const auto element = inputFunction.execute(itemRecord, pipelineMemoryProvider.arena).getRawValueAs<VariableSizedData>();
        size = size + lengthSize + element.getSize();
        count = count + 1;
    }
    auto array = pipelineMemoryProvider.arena.allocateVariableSizedData(size);
    auto position = array.getContent();
    VarVal{count}.writeToMemory(position);
    position = position + lengthSize;
    for (const auto& itemRecord : pagedVectorRef)
    {
        const auto element = inputFunction.execute(itemRecord, pipelineMemoryProvider.arena).getRawValueAs<VariableSizedData>();
        VarVal{element.getSize()}.castToType(DataType::Type::UINT32).writeToMemory(position);
        position = position + lengthSize;
        nautilus::memcpy(position, element.getContent(), element.getSize());
        position = position + element.getSize();
    }

    Record resultRecord;
    resultRecord.write(resultFieldIdentifier, VarVal{array});
    return resultRecord;
}

void ArrayAggregationPhysicalFunction::reset(
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
            throw BufferAllocationFailure("No unpooled TupleBuffer available for array aggregation paged vector!");
        },
        parentBuffer.asArg(),
        pipelineMemoryProvider.bufferProvider,
        tupleSize);
    *static_cast<nautilus::val<uint32_t*>>(aggregationState) = childBufferIndexVal;
}

void ArrayAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> /*aggregationState*/)
{
    /// No-op: the paged vector buffer is stored as a child of the parent hash map TupleBuffer and
    /// is released automatically when the parent is released.
}

size_t ArrayAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    /// uint32_t child buffer index (4B); the answer is never null, so the state holds no null flag
    return sizeof(uint32_t);
}

AggregationPhysicalFunctionRegistryReturnType
ArrayAggregationPhysicalFunction::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    INVARIANT(arguments.tupleLayout.has_value(), "Tuple layout paged vector not set");
    return std::make_shared<ArrayAggregationPhysicalFunction>(
        std::move(arguments.inputType),
        std::move(arguments.resultType),
        arguments.inputFunction,
        arguments.resultFieldIdentifier,
        arguments.tupleLayout.value());
}

}
