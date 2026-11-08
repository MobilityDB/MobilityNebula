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

#include <Aggregation/SessionAggregationOperatorHandler.hpp>

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/DefaultTimeBasedSliceStore.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

SessionAggregationOperatorHandler::SessionAggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    const uint64_t maxNumberOfBuckets,
    const uint64_t gapInMilliseconds)
    : AggregationOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore), maxNumberOfBuckets)
    , gapInMilliseconds(gapInMilliseconds)
{
    PRECONDITION(gapInMilliseconds > 0, "The gap of a session window must be positive");
    PRECONDITION(
        dynamic_cast<DefaultTimeBasedSliceStore*>(this->sliceAndWindowStore.get()) != nullptr,
        "A session window keeps its slices in a DefaultTimeBasedSliceStore");
}

SessionAggregationOperatorHandler::~SessionAggregationOperatorHandler()
{
    const std::scoped_lock lock(mutex);
    destroyHashMap(std::move(openSessions));
}

void SessionAggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
    PipelineExecutionContext* pipelineCtx)
{
    const std::scoped_lock lock(mutex);
    for (const auto& [windowInfo, slices] : slicesAndWindowInfo)
    {
        pendingSlices.emplace(windowInfo.sequenceNumber, PendingSlices{windowInfo.windowInfo, slices});
    }
    processInSequence(pipelineCtx);
}

void SessionAggregationOperatorHandler::triggerAllWindows(PipelineExecutionContext* pipelineCtx)
{
    WindowBasedOperatorHandler::triggerAllWindows(pipelineCtx);
    const std::scoped_lock lock(mutex);
    const auto* store = dynamic_cast<DefaultTimeBasedSliceStore*>(sliceAndWindowStore.get());
    if (store->getNumberOfActiveInputPipelines() == 0)
    {
        allInputPipelinesTerminated = true;
    }
    processInSequence(pipelineCtx);
}

void SessionAggregationOperatorHandler::processInSequence(PipelineExecutionContext* pipelineCtx)
{
    /// Another worker thread may trigger a slice of a later sequence number first, so a slice waits here for the ones before it
    while (not pendingSlices.empty() and pendingSlices.begin()->first == nextSequenceNumber)
    {
        chainSlices(pendingSlices.begin()->second, pipelineCtx);
        pendingSlices.erase(pendingSlices.begin());
        nextSequenceNumber = SequenceNumber(nextSequenceNumber.getRawValue() + 1);
    }

    /// After the last termination, the open sessions end once every triggered slice has been chained
    if (allInputPipelinesTerminated and not openSessionsFlushed and pendingSlices.empty())
    {
        const auto* store = dynamic_cast<DefaultTimeBasedSliceStore*>(sliceAndWindowStore.get());
        if (store->getNextSequenceNumber() == nextSequenceNumber)
        {
            openSessionsFlushed = true;
            if (openSessions)
            {
                emitSessions(std::move(openSessions), lastChainedSliceEnd, pipelineCtx);
            }
        }
    }
}

void SessionAggregationOperatorHandler::chainSlices(const PendingSlices& pending, PipelineExecutionContext* pipelineCtx)
{
    if (pending.windowInfo.windowEnd <= lastChainedSliceEnd)
    {
        /// The slice was created again for records that arrived after their slice had been chained
        NES_WARNING(
            "Session window drops the records of slice {}-{}: they arrived after the slice ending at {} was chained",
            pending.windowInfo.windowStart,
            pending.windowInfo.windowEnd,
            lastChainedSliceEnd);
        return;
    }
    INVARIANT(mergeNautilusFunction and chainNautilusFunction, "The build's setup registers the merge and chain functions");

    std::vector<Nautilus::Interface::ChainedHashMap*> threadHashMaps;
    for (const auto& slice : pending.slices)
    {
        const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
        INVARIANT(aggregationSlice != nullptr, "A session window aggregation holds AggregationSlices");
        for (uint64_t hashMapIdx = 0; hashMapIdx < aggregationSlice->getNumberOfHashMaps(); ++hashMapIdx)
        {
            if (auto* hashMap = aggregationSlice->getHashMapPtr(WorkerThreadId(hashMapIdx));
                (hashMap != nullptr) and hashMap->getNumberOfTuples() > 0)
            {
                threadHashMaps.emplace_back(dynamic_cast<Nautilus::Interface::ChainedHashMap*>(hashMap));
            }
        }
    }
    lastChainedSliceEnd = pending.windowInfo.windowEnd;

    /// The compiled functions iterate their hash maps, and a hash map with no entry has no page to iterate:
    /// the open sessions, when present, always hold an entry, and so does the merged slice
    if (threadHashMaps.empty())
    {
        /// A slice without a record of any key ends every open session
        if (openSessions)
        {
            emitSessions(std::move(openSessions), pending.windowInfo.windowStart, pipelineCtx);
        }
        return;
    }

    auto bufferProvider = pipelineCtx->getBufferManager();
    Arena arena(bufferProvider);
    auto sliceSessions = Nautilus::Interface::ChainedHashMap::createNewMapWithSameConfiguration(*threadHashMaps.front());
    for (auto* threadHashMap : threadHashMaps)
    {
        mergeNautilusFunction->operator()(sliceSessions.get(), threadHashMap, bufferProvider.get(), &arena);
    }
    if (not openSessions)
    {
        /// Every session of the slice starts in it
        openSessions = std::move(sliceSessions);
        return;
    }

    auto goingOnSessions = Nautilus::Interface::ChainedHashMap::createNewMapWithSameConfiguration(*sliceSessions);
    auto endedSessions = Nautilus::Interface::ChainedHashMap::createNewMapWithSameConfiguration(*sliceSessions);
    const auto firstOpenEventTime = chainNautilusFunction->operator()(
        openSessions.get(), sliceSessions.get(), goingOnSessions.get(), endedSessions.get(), bufferProvider.get(), &arena);

    destroyHashMap(std::move(openSessions));
    destroyHashMap(std::move(sliceSessions));
    openSessions = std::move(goingOnSessions);

    if (endedSessions->getNumberOfTuples() == 0)
    {
        endedSessions.reset();
        return;
    }
    /// No later session starts before the open ones or before this slice
    const auto watermark = Timestamp(std::min(firstOpenEventTime, pending.windowInfo.windowStart.getRawValue()));
    emitSessions(std::move(endedSessions), watermark, pipelineCtx);
}

void SessionAggregationOperatorHandler::emitSessions(
    std::unique_ptr<Nautilus::Interface::ChainedHashMap> sessions, const Timestamp watermark, PipelineExecutionContext* pipelineCtx)
{
    const auto numberOfSessions = sessions->getNumberOfTuples();
    const auto neededBufferSize = sizeof(EmittedAggregationWindow);
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the session window trigger were requested", neededBufferSize);
    }
    auto tupleBuffer = tupleBufferVal.value();
    std::ranges::fill(tupleBuffer.getAvailableMemoryArea(), std::byte{0});
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(SequenceNumber(nextEmittedSequenceNumber++));
    tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
    tupleBuffer.setLastChunk(true);
    tupleBuffer.setWatermark(watermark);
    tupleBuffer.setNumberOfTuples(numberOfSessions);
    tupleBuffer.setCreationTimestampInMS(Timestamp(
        std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::high_resolution_clock::now().time_since_epoch()).count()));

    /// The probe lowers the ended sessions from the final hash map, and has no other hash map to combine
    auto tmp = tupleBuffer.getAvailableMemoryArea();
    new (tmp.data()) EmittedAggregationWindow{WindowInfo{watermark.getRawValue(), watermark.getRawValue()}, std::move(sessions), {}};
    pipelineCtx->emitBuffer(tupleBuffer);
    NES_TRACE("Emitted {} sessions with watermarkTs {} originId {}", numberOfSessions, watermark, outputOriginId);
}

void SessionAggregationOperatorHandler::destroyHashMap(std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap) const
{
    if (hashMap and hashMap->getNumberOfTuples() > 0 and cleanupStateNautilusFunction)
    {
        cleanupStateNautilusFunction->operator()(hashMap.get());
    }
    hashMap.reset();
}

}
