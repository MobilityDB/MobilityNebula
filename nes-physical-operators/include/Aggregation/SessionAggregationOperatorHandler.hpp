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
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMap.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <SliceStore/Slice.hpp>
#include <SliceStore/WindowSlicesStoreInterface.hpp>
#include <Time/Timestamp.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <function.hpp>

namespace NES
{

/// The operator handler of a session window aggregation.
/// The build phase fills slices one gap long, as for a tumbling window of the gap. Two records of a key in one slice
/// are less than a gap apart, so they belong to one session, and a key's session goes on into the next slice when the
/// key's first record there follows its last one by less than the gap. A slice with no record of the key ends its session.
/// The handler takes the triggered slices in the order of their sequence numbers, merges the hash maps of the worker
/// threads, and chains each key's state into its open session. A session that does not go on is emitted to the probe,
/// whose window is [first event time, last event time + gap). The last termination emits every session still open.
class SessionAggregationOperatorHandler final : public AggregationOperatorHandler
{
public:
    /// Combines every entry of the second hash map into the first one
    using MergeExec = nautilus::engine::
        CallableFunction<void, Nautilus::Interface::HashMap*, Nautilus::Interface::HashMap*, AbstractBufferProvider*, Arena*>;
    /// Chains the merged slice (second) into the open sessions (first): writes the sessions that go on or start to the
    /// third hash map and the sessions that end to the fourth. Returns the smallest first event time of the third hash map.
    using ChainExec = nautilus::engine::CallableFunction<
        nautilus::val<uint64_t>,
        Nautilus::Interface::HashMap*,
        Nautilus::Interface::HashMap*,
        Nautilus::Interface::HashMap*,
        Nautilus::Interface::HashMap*,
        AbstractBufferProvider*,
        Arena*>;

    SessionAggregationOperatorHandler(
        const std::vector<OriginId>& inputOrigins,
        OriginId outputOriginId,
        std::unique_ptr<WindowSlicesStoreInterface> sliceAndWindowStore,
        uint64_t maxNumberOfBuckets,
        uint64_t gapInMilliseconds);
    ~SessionAggregationOperatorHandler() override;

    void triggerAllWindows(PipelineExecutionContext* pipelineCtx) override;

    [[nodiscard]] uint64_t getGapInMilliseconds() const { return gapInMilliseconds; }

    /// Set by the build's setup, as the cleanup function is
    std::shared_ptr<MergeExec> mergeNautilusFunction;
    std::shared_ptr<ChainExec> chainNautilusFunction;

protected:
    void triggerSlices(
        const std::map<WindowInfoAndSequenceNumber, std::vector<std::shared_ptr<Slice>>>& slicesAndWindowInfo,
        PipelineExecutionContext* pipelineCtx) override;

private:
    struct PendingSlices
    {
        WindowInfo windowInfo;
        std::vector<std::shared_ptr<Slice>> slices;
    };

    /// All four require the mutex
    void processInSequence(PipelineExecutionContext* pipelineCtx);
    void chainSlices(const PendingSlices& pendingSlices, PipelineExecutionContext* pipelineCtx);
    void
    emitSessions(std::unique_ptr<Nautilus::Interface::ChainedHashMap> sessions, Timestamp watermark, PipelineExecutionContext* pipelineCtx);
    void destroyHashMap(std::unique_ptr<Nautilus::Interface::ChainedHashMap> hashMap) const;

    std::mutex mutex;
    std::map<SequenceNumber, PendingSlices> pendingSlices;
    SequenceNumber nextSequenceNumber{SequenceNumber::INITIAL};
    SequenceNumber::Underlying nextEmittedSequenceNumber{SequenceNumber::INITIAL};
    std::unique_ptr<Nautilus::Interface::ChainedHashMap> openSessions;
    Timestamp lastChainedSliceEnd{Timestamp::INITIAL_VALUE};
    bool allInputPipelinesTerminated{false};
    bool openSessionsFlushed{false};
    uint64_t gapInMilliseconds;
};

}
