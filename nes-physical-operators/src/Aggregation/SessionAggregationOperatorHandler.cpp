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
#include <cstddef>
#include <cstdint>
#include <limits>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>
#include <variant>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Logger/Logger.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

namespace
{
uint64_t numberOfRecords(const TupleBuffer& hashMapBuffer)
{
    return ChainedHashMap::load(hashMapBuffer).getTotalNumberOfRecords();
}
}

SessionAggregationOperatorHandler::SessionAggregationOperatorHandler(
    const std::vector<OriginId>& inputOrigins,
    const OriginId outputOriginId,
    std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
    ChainedHashMapConfig hashMapConfig,
    std::shared_ptr<SessionFunctions> sessionFunctions)
    : AggregationOperatorHandler(inputOrigins, outputOriginId, std::move(sliceAndWindowStore))
    , hashMapConfig(std::move(hashMapConfig))
    , sessionFunctions(std::move(sessionFunctions))
{
    PRECONDITION(this->sessionFunctions != nullptr, "A session window aggregation needs the holder of its compiled functions");
}

void SessionAggregationOperatorHandler::checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx)
{
    /// As WindowBasedOperatorHandler::checkAndTriggerWindows, but the forwarded watermark closes sessions before it goes on:
    /// a session may have started long before the watermark the slice store forwards
    const auto newGlobalWatermark
        = watermarkProcessorBuild->updateWatermark(bufferMetaData.watermarkTs, bufferMetaData.seqNumber, bufferMetaData.originId);
    const auto newValue = newGlobalWatermark.getRawValue();
    auto highestWatermark = highestTriggeredWatermark.load();
    while (true)
    {
        if (newValue <= highestWatermark)
        {
            return;
        }
        if (highestTriggeredWatermark.compare_exchange_weak(highestWatermark, newValue))
        {
            break;
        }
    }

    const auto [slicesAndWindowInfo, outputWatermarkAndSequenceNumber] = sliceAndWindowStore->getTriggerableWindowSlices(newGlobalWatermark);
    const std::scoped_lock lock(mutex);
    for (const auto& [windowInfo, slices] : slicesAndWindowInfo)
    {
        pendingItems.emplace(windowInfo.sequenceNumber, PendingSlice{.windowInfo = windowInfo.windowInfo, .slices = slices});
    }
    if (outputWatermarkAndSequenceNumber)
    {
        /// A record not chained yet lies in a slice that ends at or after the global watermark, so its event time is at least
        /// the watermark minus the gap, the slices' size: the sessions close there
        pendingItems.emplace(outputWatermarkAndSequenceNumber->second, newGlobalWatermark - sliceAndWindowStore->getWindowSize());
    }
    processInSequence(pipelineCtx);
}

void SessionAggregationOperatorHandler::triggerSlices(
    const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>&, PipelineExecutionContext*)
{
    INVARIANT(false, "A session window takes its slices in checkAndTriggerWindows");
}

void SessionAggregationOperatorHandler::processInSequence(PipelineExecutionContext* pipelineCtx)
{
    /// Another worker thread may trigger a later sequence number first, so an item waits here for the ones before it
    while (not pendingItems.empty() and pendingItems.begin()->first == nextSequenceNumber)
    {
        const auto& [sequenceNumber, item] = *pendingItems.begin();
        if (const auto* pending = std::get_if<PendingSlice>(&item))
        {
            chainSlice(*pending, sequenceNumber, pipelineCtx);
        }
        else
        {
            closeSessions(std::get<Timestamp>(item), sequenceNumber, pipelineCtx);
        }
        pendingItems.erase(pendingItems.begin());
        nextSequenceNumber = SequenceNumber(nextSequenceNumber.getRawValue() + 1);
    }
}

TupleBuffer SessionAggregationOperatorHandler::createHashMap(AbstractBufferProvider& bufferProvider) const
{
    const auto neededBufferSize = ChainedHashMap::calculateBufferSize(hashMapConfig.numberOfBuckets, hashMapConfig.bloomFilterMemAreaSize());
    auto buffer = bufferProvider.getUnpooledBuffer(neededBufferSize);
    if (not buffer.has_value())
    {
        throw CannotAllocateBuffer("{}B for a hash map of session windows were requested", neededBufferSize);
    }
    ChainedHashMap::init(buffer.value(), hashMapConfig);
    return buffer.value();
}

void SessionAggregationOperatorHandler::chainSlice(
    const PendingSlice& pending, const SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx)
{
    const auto watermarkOfSlice = Timestamp(std::min(
        openSessions ? firstOpenEventTime : std::numeric_limits<uint64_t>::max(), pending.windowInfo.windowStart.getRawValue()));
    if (pending.windowInfo.windowEnd <= lastChainedSliceEnd)
    {
        /// The slice was created again for records that arrived after their slice had been chained
        NES_WARNING(
            "Session window drops the records of slice {}-{}: they arrived after the slice ending at {} was chained",
            pending.windowInfo.windowStart,
            pending.windowInfo.windowEnd,
            lastChainedSliceEnd);
        emitSessions(std::nullopt, watermarkOfSlice, sequenceNumber, pipelineCtx);
        return;
    }
    lastChainedSliceEnd = pending.windowInfo.windowEnd;

    std::vector<TupleBuffer> threadHashMaps;
    for (const auto& slice : pending.slices)
    {
        const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(slice);
        INVARIANT(aggregationSlice != nullptr, "A session window aggregation holds AggregationSlices");
        for (uint64_t hashMapIdx = 0; hashMapIdx < aggregationSlice->getNumberOfHashMaps(); ++hashMapIdx)
        {
            if (const TupleBuffer* hashMapBuffer = aggregationSlice->getHashMapBufferRefForWorker(WorkerThreadId(hashMapIdx));
                hashMapBuffer != nullptr and numberOfRecords(*hashMapBuffer) > 0)
            {
                threadHashMaps.emplace_back(*hashMapBuffer);
            }
        }
    }
    if (threadHashMaps.empty())
    {
        emitSessions(std::nullopt, watermarkOfSlice, sequenceNumber, pipelineCtx);
        return;
    }

    /// The compiled functions iterate their hash maps, and a hash map with no entry has no page to iterate:
    /// the open sessions, when present, always hold an entry, and so does the merged slice
    const std::scoped_lock functionsLock(sessionFunctions->mutex);
    INVARIANT(sessionFunctions->merge and sessionFunctions->chain, "The build's setup registers the session functions");
    auto bufferProvider = pipelineCtx->getBufferManager();
    Arena arena(bufferProvider);
    auto sliceSessions = createHashMap(*bufferProvider);
    for (auto& threadHashMap : threadHashMaps)
    {
        (*sessionFunctions->merge)(&sliceSessions, &threadHashMap, bufferProvider.get(), &arena);
    }
    if (not openSessions)
    {
        /// Every session of the slice starts in it
        openSessions = std::move(sliceSessions);
        firstOpenEventTime = pending.windowInfo.windowStart.getRawValue();
        emitSessions(std::nullopt, pending.windowInfo.windowStart, sequenceNumber, pipelineCtx);
        return;
    }

    auto goingOnSessions = createHashMap(*bufferProvider);
    auto endedSessions = createHashMap(*bufferProvider);
    firstOpenEventTime = (*sessionFunctions->chain)(
        &openSessions.value(), &sliceSessions, &goingOnSessions, &endedSessions, bufferProvider.get(), &arena);
    openSessions = std::move(goingOnSessions);

    /// No later session starts before the open ones or before this slice
    const auto watermark = Timestamp(std::min(firstOpenEventTime, pending.windowInfo.windowStart.getRawValue()));
    emitSessions(
        numberOfRecords(endedSessions) > 0 ? std::optional(std::move(endedSessions)) : std::nullopt, watermark, sequenceNumber, pipelineCtx);
}

void SessionAggregationOperatorHandler::closeSessions(
    const Timestamp watermark, const SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx)
{
    if (not openSessions)
    {
        emitSessions(std::nullopt, watermark, sequenceNumber, pipelineCtx);
        return;
    }
    const std::scoped_lock functionsLock(sessionFunctions->mutex);
    INVARIANT(sessionFunctions->close, "The build's setup registers the session functions");
    auto bufferProvider = pipelineCtx->getBufferManager();
    Arena arena(bufferProvider);
    auto stillOpenSessions = createHashMap(*bufferProvider);
    auto endedSessions = createHashMap(*bufferProvider);
    const auto firstStillOpen = (*sessionFunctions->close)(
        &openSessions.value(), &stillOpenSessions, &endedSessions, watermark.getRawValue(), bufferProvider.get(), &arena);

    if (numberOfRecords(stillOpenSessions) > 0)
    {
        openSessions = std::move(stillOpenSessions);
        firstOpenEventTime = firstStillOpen;
    }
    else
    {
        openSessions.reset();
    }
    const auto outputWatermark = openSessions ? Timestamp(std::min(firstOpenEventTime, watermark.getRawValue())) : watermark;
    emitSessions(
        numberOfRecords(endedSessions) > 0 ? std::optional(std::move(endedSessions)) : std::nullopt,
        outputWatermark,
        sequenceNumber,
        pipelineCtx);
}

void SessionAggregationOperatorHandler::emitSessions(
    std::optional<TupleBuffer> sessions, const Timestamp watermark, const SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx)
{
    /// The watermark a session window forwards never goes back
    lastEmittedWatermark = std::max(lastEmittedWatermark, watermark);
    const auto numberOfSessions = sessions ? numberOfRecords(*sessions) : 0;
    constexpr auto neededBufferSize = sizeof(EmittedAggregationWindow);
    const auto tupleBufferVal = pipelineCtx->getBufferManager()->getUnpooledBuffer(neededBufferSize);
    if (not tupleBufferVal.has_value())
    {
        throw CannotAllocateBuffer("{}B for the session window trigger were requested", neededBufferSize);
    }
    auto tupleBuffer = tupleBufferVal.value();
    if (sessions)
    {
        /// The probe loads the ended sessions as its one hash map
        std::ignore = tupleBuffer.storeChildBuffer(*sessions);
    }
    std::ranges::fill(tupleBuffer.getAvailableMemoryArea(), std::byte{0});
    tupleBuffer.setOriginId(outputOriginId);
    tupleBuffer.setSequenceNumber(sequenceNumber);
    tupleBuffer.setChunkNumber(ChunkNumber(ChunkNumber::INITIAL));
    tupleBuffer.setLastChunk(true);
    tupleBuffer.setWatermark(lastEmittedWatermark);
    tupleBuffer.setNumberOfTuples(numberOfSessions);
    tupleBuffer.setCreationTimestampInMS(Timestamp::nowInMilliseconds());

    /// Each session writes its own window, so the window of the emitted buffer is only its watermark
    auto tmp = tupleBuffer.getAvailableMemoryArea();
    new (tmp.data()) EmittedAggregationWindow{
        WindowInfo{lastEmittedWatermark.getRawValue(), lastEmittedWatermark.getRawValue()}, sessions ? uint64_t{1} : uint64_t{0}};
    pipelineCtx->emitBuffer(tupleBuffer);
    NES_TRACE(
        "Emitted {} sessions with watermarkTs {} sequenceNumber {} originId {}", numberOfSessions, lastEmittedWatermark, sequenceNumber, outputOriginId);
}

}
