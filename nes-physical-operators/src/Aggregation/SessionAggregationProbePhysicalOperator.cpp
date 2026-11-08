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

#include <Aggregation/SessionAggregationProbePhysicalOperator.hpp>

#include <cstdint>
#include <memory>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/SessionBoundsAggregationPhysicalFunction.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Time/Timestamp.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <WindowProbePhysicalOperator.hpp>
#include <function.hpp>
#include <static.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

void SessionAggregationProbePhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// As this operator functions as a scan, we have to set the execution context for this pipeline
    executionCtx.watermarkTs = recordBuffer.getWatermarkTs();
    executionCtx.currentTs = recordBuffer.getCreatingTs();
    executionCtx.sequenceNumber = recordBuffer.getSequenceNumber();
    executionCtx.chunkNumber = recordBuffer.getChunkNumber();
    executionCtx.lastChunk = recordBuffer.isLastChunk();
    executionCtx.originId = recordBuffer.getOriginId();
    openChild(executionCtx, recordBuffer);

    /// The handler emits the ended sessions as the final hash map
    const auto aggregationWindowRef = static_cast<nautilus::val<EmittedAggregationWindow*>>(recordBuffer.getMemArea());
    auto finalHashMapPtr = Nautilus::Util::readValueFromMemRef<Interface::HashMap*>(
        Nautilus::Util::getMemberRef(aggregationWindowRef, &EmittedAggregationWindow::finalHashMapPtr));
    const Interface::ChainedHashMapRef finalHashMap(
        finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

    /// The bounds of the session are the state of the last aggregation function
    const auto numberOfQueryFunctions = aggregationPhysicalFunctions.size() - 1;
    size_t boundsOffset = 0;
    for (size_t i = 0; i < numberOfQueryFunctions; ++i)
    {
        boundsOffset += aggregationPhysicalFunctions[i]->getSizeOfStateInBytes();
    }
    const std::vector queryFunctions(aggregationPhysicalFunctions.begin(), aggregationPhysicalFunctions.begin() + numberOfQueryFunctions);

    for (const auto entry : finalHashMap)
    {
        const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
            entry, finalHashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
        const auto recordKey = entryRef.getKey();
        const auto statePtr = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
        Record outputRecord;
        auto finalStatePtr = statePtr;
        for (const auto& aggFunction : nautilus::static_iterable(queryFunctions))
        {
            outputRecord.reassignFields(aggFunction->lower(finalStatePtr, executionCtx.pipelineMemoryProvider));
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
        }

        const auto boundsPtr = statePtr + nautilus::val<uint64_t>(boundsOffset);
        const nautilus::val<Timestamp> windowStart{SessionBoundsAggregationPhysicalFunction::readFirst(boundsPtr)};
        const nautilus::val<Timestamp> windowEnd{
            SessionBoundsAggregationPhysicalFunction::readLast(boundsPtr) + nautilus::val<uint64_t>(gapInMilliseconds)};
        outputRecord.reassignFields(recordKey);
        outputRecord.write(windowMetaData.windowStartFieldName, windowStart.convertToValue());
        outputRecord.write(windowMetaData.windowEndFieldName, windowEnd.convertToValue());
        executeChild(executionCtx, outputRecord);

        finalStatePtr = statePtr;
        for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
        {
            aggFunction->cleanup(finalStatePtr);
            finalStatePtr = finalStatePtr + aggFunction->getSizeOfStateInBytes();
        }
    }

    /// The probe owns the emitted sessions, so it destroys their hash map
    nautilus::invoke(
        +[](EmittedAggregationWindow* emittedAggregationWindow) { emittedAggregationWindow->finalHashMap.reset(); }, aggregationWindowRef);
}

SessionAggregationProbePhysicalOperator::SessionAggregationProbePhysicalOperator(
    HashMapOptions hashMapOptions,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationPhysicalFunctions,
    const OperatorHandlerId operatorHandlerId,
    WindowMetaData windowMetaData,
    const uint64_t gapInMilliseconds)
    : WindowProbePhysicalOperator(operatorHandlerId, std::move(windowMetaData))
    , aggregationPhysicalFunctions(std::move(aggregationPhysicalFunctions))
    , hashMapOptions(std::move(hashMapOptions))
    , gapInMilliseconds(gapInMilliseconds)
{
    PRECONDITION(not this->aggregationPhysicalFunctions.empty(), "A session window aggregation holds at least its bounds function");
}

}
