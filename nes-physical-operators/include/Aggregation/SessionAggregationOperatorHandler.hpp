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

#pragma once

#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <WindowBasedOperatorHandler.hpp>

namespace NES
{

/// The compiled functions over hash-map buffers a session window needs. The build's setup registers them, and the
/// SessionAggregationOperatorHandler calls them; the lowering hands both the same holder.
struct SessionFunctions
{
    /// Combines every entry of the second hash map into the first one
    using Merge = PipelineFunction<void(TupleBuffer*, TupleBuffer*, AbstractBufferProvider*, Arena*)>;
    /// Chains the merged slice (second) into the open sessions (first): writes the sessions that go on or start to the
    /// third hash map and the sessions that end to the fourth. Returns the smallest first event time of the third.
    using Chain = PipelineFunction<uint64_t(TupleBuffer*, TupleBuffer*, TupleBuffer*, TupleBuffer*, AbstractBufferProvider*, Arena*)>;
    /// Splits the open sessions (first) at a watermark (fourth): a session whose last event time plus the gap lies
    /// below the watermark has ended and goes to the third hash map, any other goes to the second. Returns the
    /// smallest first event time of the second.
    using Close = PipelineFunction<uint64_t(TupleBuffer*, TupleBuffer*, TupleBuffer*, uint64_t, AbstractBufferProvider*, Arena*)>;

    std::mutex mutex;
    std::optional<Merge> merge;
    std::optional<Chain> chain;
    std::optional<Close> close;
};

/// The operator handler of a session window aggregation.
/// The build fills slices one gap long, as for a tumbling window of the gap. Two records of a key in one slice are
/// at most a gap apart, so they belong to one session, and a key's session goes on into a later slice when the key's
/// first record there follows its last one by at most the gap. The handler takes, in the order of the sequence numbers
/// the slice store assigns, each triggered slice and each forwarded watermark: it merges the hash maps of a slice's
/// worker threads and chains each key's state into its open session, and a watermark W ends every session whose last
/// event time plus the gap lies below W minus the gap, the least event time a record of a slice not yet triggered can
/// have, the end of a stream ending them all. It emits one buffer per sequence number,
/// holding the sessions that ended, to the probe, which writes the window [first event time, last event time + gap).
class SessionAggregationOperatorHandler final : public AggregationOperatorHandler
{
public:
    SessionAggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        ChainedHashMapConfig hashMapConfig,
        std::shared_ptr<SessionFunctions> sessionFunctions);

    void checkAndTriggerWindows(const BufferMetaData& bufferMetaData, PipelineExecutionContext* pipelineCtx) override;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

private:
    struct PendingSlice
    {
        WindowInfo windowInfo;
        std::vector<std::shared_ptr<Slice>> slices;
    };

    /// A slice to chain, or the event time below which no record remains to chain, to close the open sessions at
    using PendingItem = std::variant<PendingSlice, Timestamp>;

    /// All require the mutex
    void processInSequence(PipelineExecutionContext* pipelineCtx);
    void chainSlice(const PendingSlice& pending, SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx);
    void closeSessions(Timestamp watermark, SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx);
    [[nodiscard]] TupleBuffer createHashMap(AbstractBufferProvider& bufferProvider) const;
    void emitSessions(
        std::optional<TupleBuffer> sessions, Timestamp watermark, SequenceNumber sequenceNumber, PipelineExecutionContext* pipelineCtx);

    std::mutex mutex;
    std::map<SequenceNumber, PendingItem> pendingItems;
    SequenceNumber nextSequenceNumber{SequenceNumber::INITIAL};
    std::optional<TupleBuffer> openSessions;
    /// A lower bound of the first event times of the open sessions, when there is one
    uint64_t firstOpenEventTime{0};
    Timestamp lastChainedSliceEnd{Timestamp::INITIAL_VALUE};
    Timestamp lastEmittedWatermark{Timestamp::INITIAL_VALUE};
    ChainedHashMapConfig hashMapConfig;
    std::shared_ptr<SessionFunctions> sessionFunctions;
};

}
