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
#include <Aggregation/AggregationBuildPhysicalOperator.hpp>

#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/SessionBoundsAggregationPhysicalFunction.hpp>
#include <Aggregation/SessionAggregationOperatorHandler.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapConfig.hpp>
#include <Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Interface/HashMap/HashMap.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
#include <Arena.hpp>
#include <CompilationContext.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <HashMapSlice.hpp>
#include <WindowBuildPhysicalOperator.hpp>
#include <function.hpp>
#include <options.hpp>
#include <static.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
using AggregationFunctions = std::vector<std::shared_ptr<AggregationPhysicalFunction>>;

/// Combines the states of an entry into the entry of the same key in another hash map, creating it reset when absent
void combineEntryInto(
    ChainedHashMapRef& target,
    const BorrowedNautilusBuffer& targetBuffer,
    const ChainedHashMapRef::ChainedEntryRef& sourceEntry,
    const ChainedHashMapConfig& config,
    const AggregationFunctions& functions,
    PipelineMemoryProvider& memoryProvider)
{
    const auto combineStates = [&](const nautilus::val<AbstractHashMapEntry*>& targetEntry, const bool resetFirst)
    {
        const ChainedHashMapRef::ChainedEntryRef targetEntryRef{targetEntry, targetBuffer, config.fieldKeys, config.fieldValues};
        auto targetState = static_cast<nautilus::val<AggregationState*>>(targetEntryRef.getValueMemArea());
        auto sourceState = static_cast<nautilus::val<AggregationState*>>(sourceEntry.getValueMemArea());
        for (const auto& function : nautilus::static_iterable(functions))
        {
            if (resetFirst)
            {
                function->reset(targetState, targetBuffer, memoryProvider);
            }
            function->combine(targetState, targetBuffer, sourceState, sourceEntry.hashMapBuffer, memoryProvider);
            targetState = targetState + function->getSizeOfStateInBytes();
            sourceState = sourceState + function->getSizeOfStateInBytes();
        }
    };
    target.insertOrUpdateEntry(
        sourceEntry.entryRef,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry) { combineStates(entry, false); },
        [&](const nautilus::val<AbstractHashMapEntry*>& entry) { combineStates(entry, true); },
        memoryProvider.bufferProvider);
}

/// Registers the merge, chain and close functions of a session window, see SessionAggregationOperatorHandler.
/// The compiled functions cannot take const or reference parameters.
/// NOLINTBEGIN(performance-unnecessary-value-param)
void registerSessionFunctions(
    SessionFunctions& sessionFunctions,
    CompilationContext& compilationContext,
    const ChainedHashMapConfig& hashMapConfig,
    const AggregationFunctions& functions,
    const uint64_t gapInMilliseconds)
{
    sessionFunctions.merge.emplace(compilationContext.registerFunction(
        std::function(
            [config = hashMapConfig, functions](
                nautilus::val<TupleBuffer*> target,
                nautilus::val<TupleBuffer*> source,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<Arena*> arena)
            {
                PipelineMemoryProvider memoryProvider(arena, bufferProvider);
                const BorrowedNautilusBuffer targetBuffer = BorrowedNautilusBuffer::from(target);
                const BorrowedNautilusBuffer sourceBuffer = BorrowedNautilusBuffer::from(source);
                ChainedHashMapRef targetRef{targetBuffer, config};
                const ChainedHashMapRef sourceRef{sourceBuffer, config};
                for (const auto entry : sourceRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef sourceEntry{entry, sourceBuffer, config.fieldKeys, config.fieldValues};
                    combineEntryInto(targetRef, targetBuffer, sourceEntry, config, functions, memoryProvider);
                }
            }),
        "sessionMerge"));

    sessionFunctions.chain.emplace(compilationContext.registerFunction(
        std::function(
            [config = hashMapConfig, functions, boundsOffset = SessionBoundsAggregationPhysicalFunction::stateOffset(functions), gapInMilliseconds](
                nautilus::val<TupleBuffer*> open,
                nautilus::val<TupleBuffer*> slice,
                nautilus::val<TupleBuffer*> goingOn,
                nautilus::val<TupleBuffer*> ended,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<Arena*> arena) -> nautilus::val<uint64_t>
            {
                PipelineMemoryProvider memoryProvider(arena, bufferProvider);
                const BorrowedNautilusBuffer openBuffer = BorrowedNautilusBuffer::from(open);
                const BorrowedNautilusBuffer sliceBuffer = BorrowedNautilusBuffer::from(slice);
                const BorrowedNautilusBuffer goingOnBuffer = BorrowedNautilusBuffer::from(goingOn);
                const BorrowedNautilusBuffer endedBuffer = BorrowedNautilusBuffer::from(ended);
                ChainedHashMapRef openRef{openBuffer, config};
                const ChainedHashMapRef sliceRef{sliceBuffer, config};
                ChainedHashMapRef goingOnRef{goingOnBuffer, config};
                ChainedHashMapRef endedRef{endedBuffer, config};
                const auto bounds = [&](const ChainedHashMapRef::ChainedEntryRef& entryRef)
                {
                    return static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea())
                        + nautilus::val<uint64_t>(boundsOffset);
                };

                /// A key of the slice goes on with its open session when its first record follows the session's last one
                /// by at most the gap, as Flink merges two session windows that touch; otherwise it starts a session
                for (const auto entry : sliceRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef sliceEntry{entry, sliceBuffer, config.fieldKeys, config.fieldValues};
                    if (const auto openEntry = openRef.findEntry(sliceEntry.entryRef); openEntry != nullptr)
                    {
                        const ChainedHashMapRef::ChainedEntryRef openEntryRef{openEntry, openBuffer, config.fieldKeys, config.fieldValues};
                        if (SessionBoundsAggregationPhysicalFunction::readFirst(bounds(sliceEntry))
                            <= SessionBoundsAggregationPhysicalFunction::readLast(bounds(openEntryRef))
                                + nautilus::val<uint64_t>(gapInMilliseconds))
                        {
                            combineEntryInto(goingOnRef, goingOnBuffer, openEntryRef, config, functions, memoryProvider);
                        }
                    }
                    combineEntryInto(goingOnRef, goingOnBuffer, sliceEntry, config, functions, memoryProvider);
                }

                /// An open session that did not go on has ended: the key's session in goingOn, if any, starts later
                for (const auto entry : openRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef openEntry{entry, openBuffer, config.fieldKeys, config.fieldValues};
                    nautilus::val<bool> hasEnded(true);
                    if (const auto goingOnEntry = goingOnRef.findEntry(openEntry.entryRef); goingOnEntry != nullptr)
                    {
                        const ChainedHashMapRef::ChainedEntryRef goingOnEntryRef{
                            goingOnEntry, goingOnBuffer, config.fieldKeys, config.fieldValues};
                        if (SessionBoundsAggregationPhysicalFunction::readFirst(bounds(goingOnEntryRef))
                            == SessionBoundsAggregationPhysicalFunction::readFirst(bounds(openEntry)))
                        {
                            hasEnded = false;
                        }
                    }
                    if (hasEnded)
                    {
                        combineEntryInto(endedRef, endedBuffer, openEntry, config, functions, memoryProvider);
                    }
                }

                nautilus::val<uint64_t> firstOpenEventTime(std::numeric_limits<uint64_t>::max());
                for (const auto entry : goingOnRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef goingOnEntry{entry, goingOnBuffer, config.fieldKeys, config.fieldValues};
                    const auto first = SessionBoundsAggregationPhysicalFunction::readFirst(bounds(goingOnEntry));
                    if (first < firstOpenEventTime)
                    {
                        firstOpenEventTime = first;
                    }
                }
                return firstOpenEventTime;
            }),
        "sessionChain"));

    sessionFunctions.close.emplace(compilationContext.registerFunction(
        std::function(
            [config = hashMapConfig, functions, boundsOffset = SessionBoundsAggregationPhysicalFunction::stateOffset(functions), gapInMilliseconds](
                nautilus::val<TupleBuffer*> open,
                nautilus::val<TupleBuffer*> stillOpen,
                nautilus::val<TupleBuffer*> ended,
                nautilus::val<uint64_t> watermark,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<Arena*> arena) -> nautilus::val<uint64_t>
            {
                PipelineMemoryProvider memoryProvider(arena, bufferProvider);
                const BorrowedNautilusBuffer openBuffer = BorrowedNautilusBuffer::from(open);
                const BorrowedNautilusBuffer stillOpenBuffer = BorrowedNautilusBuffer::from(stillOpen);
                const BorrowedNautilusBuffer endedBuffer = BorrowedNautilusBuffer::from(ended);
                const ChainedHashMapRef openRef{openBuffer, config};
                ChainedHashMapRef stillOpenRef{stillOpenBuffer, config};
                ChainedHashMapRef endedRef{endedBuffer, config};

                /// Every record still to chain has an event time at or above the watermark, so a session whose last event
                /// time plus the gap lies below it cannot go on
                nautilus::val<uint64_t> firstOpenEventTime(std::numeric_limits<uint64_t>::max());
                for (const auto entry : openRef)
                {
                    const ChainedHashMapRef::ChainedEntryRef openEntry{entry, openBuffer, config.fieldKeys, config.fieldValues};
                    const auto bounds = static_cast<nautilus::val<AggregationState*>>(openEntry.getValueMemArea())
                        + nautilus::val<uint64_t>(boundsOffset);
                    if (SessionBoundsAggregationPhysicalFunction::readLast(bounds) + nautilus::val<uint64_t>(gapInMilliseconds) < watermark)
                    {
                        combineEntryInto(endedRef, endedBuffer, openEntry, config, functions, memoryProvider);
                    }
                    else
                    {
                        combineEntryInto(stillOpenRef, stillOpenBuffer, openEntry, config, functions, memoryProvider);
                        const auto first = SessionBoundsAggregationPhysicalFunction::readFirst(bounds);
                        if (first < firstOpenEventTime)
                        {
                            firstOpenEventTime = first;
                        }
                    }
                }
                return firstOpenEventTime;
            }),
        "sessionClose"));
}
/// NOLINTEND(performance-unnecessary-value-param)
}

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);
    if (sessionFunctions)
    {
        /// Every pipeline of the build compiles them; the handler calls the ones registered first
        const std::scoped_lock lock(sessionFunctions->mutex);
        if (not sessionFunctions->merge)
        {
            registerSessionFunctions(*sessionFunctions, compilationContext, hashMapConfig, aggregationPhysicalFunctions, sessionGapInMilliseconds);
        }
    }
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the corresponding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    failIfOutOfOrder(ctx, timestamp);
    const auto hashMapBuffer
        = sliceStoreRef->getDataStructureRef(timestamp, ctx.workerThreadId, operatorHandler, ctx.pipelineMemoryProvider.bufferProvider);
    const auto borrowedHashMapBuffer = BorrowedNautilusBuffer::from(hashMapBuffer.asArg());
    ChainedHashMapRef hashMap{borrowedHashMapBuffer, hashMapConfig};

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapConfig.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapConfig.fieldKeys[i];
        const auto& function = keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        [&](const nautilus::val<AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const ChainedHashMapRef::ChainedEntryRef entryRefReset{
                entry, borrowedHashMapBuffer, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);

    /// Updating the aggregation states
    const ChainedHashMapRef::ChainedEntryRef entryRef{
        hashMapEntry, borrowedHashMapBuffer, hashMapConfig.fieldKeys, hashMapConfig.fieldValues};
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, borrowedHashMapBuffer, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::unique_ptr<SliceStoreRef> sliceStoreRef,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    ChainedHashMapConfig hashMapConfig,
    std::vector<PhysicalFunction> keyFunctions,
    std::shared_ptr<SessionFunctions> sessionFunctions,
    const uint64_t sessionGapInMilliseconds)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction), std::move(sliceStoreRef))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapConfig(std::move(hashMapConfig))
    , keyFunctions(std::move(keyFunctions))
    , sessionFunctions(std::move(sessionFunctions))
    , sessionGapInMilliseconds(sessionGapInMilliseconds)
{
}

}
