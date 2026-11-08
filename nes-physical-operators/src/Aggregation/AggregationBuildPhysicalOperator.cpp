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

#include <cstddef>
#include <cstdint>
#include <functional>
#include <limits>
#include <memory>
#include <ranges>
#include <utility>
#include <vector>
#include <Aggregation/AggregationOperatorHandler.hpp>
#include <Aggregation/AggregationSlice.hpp>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Aggregation/Function/SessionBoundsAggregationPhysicalFunction.hpp>
#include <Aggregation/SessionAggregationOperatorHandler.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Nautilus/Interface/HashMap/ChainedHashMap/ChainedHashMapRef.hpp>
#include <Nautilus/Interface/HashMap/HashMap.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <SliceStore/Slice.hpp>
#include <Time/Timestamp.hpp>
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
    Interface::ChainedHashMapRef& target,
    const nautilus::val<Interface::HashMap*>& targetPtr,
    const Interface::ChainedHashMapRef::ChainedEntryRef& sourceEntry,
    const HashMapOptions& options,
    const AggregationFunctions& functions,
    PipelineMemoryProvider& memoryProvider)
{
    const auto combineStates = [&](const nautilus::val<Interface::AbstractHashMapEntry*>& targetEntry, const bool resetFirst)
    {
        const Interface::ChainedHashMapRef::ChainedEntryRef targetEntryRef(targetEntry, targetPtr, options.fieldKeys, options.fieldValues);
        auto targetState = static_cast<nautilus::val<AggregationState*>>(targetEntryRef.getValueMemArea());
        auto sourceState = static_cast<nautilus::val<AggregationState*>>(sourceEntry.getValueMemArea());
        for (const auto& function : nautilus::static_iterable(functions))
        {
            if (resetFirst)
            {
                function->reset(targetState, memoryProvider);
            }
            function->combine(targetState, sourceState, memoryProvider);
            targetState = targetState + function->getSizeOfStateInBytes();
            sourceState = sourceState + function->getSizeOfStateInBytes();
        }
    };
    target.insertOrUpdateEntry(
        sourceEntry.entryRef,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry) { combineStates(entry, false); },
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry) { combineStates(entry, true); },
        memoryProvider.bufferProvider);
}

/// The offset of the session bounds, the state of the last aggregation function
size_t sessionBoundsOffset(const AggregationFunctions& functions)
{
    size_t offset = 0;
    for (size_t i = 0; i + 1 < functions.size(); ++i)
    {
        offset += functions[i]->getSizeOfStateInBytes();
    }
    return offset;
}

/// Registers the merge and chain functions of a session window, see SessionAggregationOperatorHandler
/// NOLINTBEGIN(performance-unnecessary-value-param)
void registerSessionFunctions(
    SessionAggregationOperatorHandler& handler,
    const CompilationContext& compilationContext,
    const HashMapOptions& hashMapOptions,
    const AggregationFunctions& functions,
    const uint64_t gapInMilliseconds)
{
    handler.mergeNautilusFunction = std::make_shared<SessionAggregationOperatorHandler::MergeExec>(
        compilationContext.registerFunction(std::function(
            [options = hashMapOptions, functions](
                nautilus::val<Interface::HashMap*> target,
                nautilus::val<Interface::HashMap*> source,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<Arena*> arena)
            {
                PipelineMemoryProvider memoryProvider(arena, bufferProvider);
                Interface::ChainedHashMapRef targetRef(target, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                const Interface::ChainedHashMapRef sourceRef(
                    source, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                for (const auto entry : sourceRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef sourceEntry(entry, source, options.fieldKeys, options.fieldValues);
                    combineEntryInto(targetRef, target, sourceEntry, options, functions, memoryProvider);
                }
            })));

    handler.chainNautilusFunction = std::make_shared<SessionAggregationOperatorHandler::ChainExec>(
        compilationContext.registerFunction(std::function(
            [options = hashMapOptions, functions, boundsOffset = sessionBoundsOffset(functions), gapInMilliseconds](
                nautilus::val<Interface::HashMap*> open,
                nautilus::val<Interface::HashMap*> slice,
                nautilus::val<Interface::HashMap*> goingOn,
                nautilus::val<Interface::HashMap*> ended,
                nautilus::val<AbstractBufferProvider*> bufferProvider,
                nautilus::val<Arena*> arena) -> nautilus::val<uint64_t>
            {
                PipelineMemoryProvider memoryProvider(arena, bufferProvider);
                Interface::ChainedHashMapRef openRef(open, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                const Interface::ChainedHashMapRef sliceRef(
                    slice, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                Interface::ChainedHashMapRef goingOnRef(
                    goingOn, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                Interface::ChainedHashMapRef endedRef(ended, options.fieldKeys, options.fieldValues, options.entriesPerPage, options.entrySize);
                const auto bounds = [&](const Interface::ChainedHashMapRef::ChainedEntryRef& entryRef)
                { return static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea()) + nautilus::val<uint64_t>(boundsOffset); };

                /// A key of the slice goes on with its open session when its first record follows the session's last one
                /// by at most the gap, as Flink merges two session windows that touch; otherwise it starts a session
                for (const auto entry : sliceRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef sliceEntry(entry, slice, options.fieldKeys, options.fieldValues);
                    if (auto openEntry = openRef.findEntry(sliceEntry.entryRef))
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef openEntryRef(
                            openEntry, open, options.fieldKeys, options.fieldValues);
                        if (SessionBoundsAggregationPhysicalFunction::readFirst(bounds(sliceEntry))
                            <= SessionBoundsAggregationPhysicalFunction::readLast(bounds(openEntryRef)) + nautilus::val<uint64_t>(gapInMilliseconds))
                        {
                            combineEntryInto(goingOnRef, goingOn, openEntryRef, options, functions, memoryProvider);
                        }
                    }
                    combineEntryInto(goingOnRef, goingOn, sliceEntry, options, functions, memoryProvider);
                }

                /// An open session that did not go on has ended: the key's session in goingOn, if any, starts later
                for (const auto entry : openRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef openEntry(entry, open, options.fieldKeys, options.fieldValues);
                    nautilus::val<bool> hasEnded(true);
                    if (auto goingOnEntry = goingOnRef.findEntry(openEntry.entryRef))
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef goingOnEntryRef(
                            goingOnEntry, goingOn, options.fieldKeys, options.fieldValues);
                        if (SessionBoundsAggregationPhysicalFunction::readFirst(bounds(goingOnEntryRef))
                            == SessionBoundsAggregationPhysicalFunction::readFirst(bounds(openEntry)))
                        {
                            hasEnded = false;
                        }
                    }
                    if (hasEnded)
                    {
                        combineEntryInto(endedRef, ended, openEntry, options, functions, memoryProvider);
                    }
                }

                nautilus::val<uint64_t> firstOpenEventTime(std::numeric_limits<uint64_t>::max());
                for (const auto entry : goingOnRef)
                {
                    const Interface::ChainedHashMapRef::ChainedEntryRef goingOnEntry(entry, goingOn, options.fieldKeys, options.fieldValues);
                    const auto first = SessionBoundsAggregationPhysicalFunction::readFirst(bounds(goingOnEntry));
                    if (first < firstOpenEventTime)
                    {
                        firstOpenEventTime = first;
                    }
                }
                return firstOpenEventTime;
            })));
}
/// NOLINTEND(performance-unnecessary-value-param)
}

Interface::HashMap* getAggHashMapProxy(
    const AggregationOperatorHandler* operatorHandler,
    const Timestamp timestamp,
    const WorkerThreadId workerThreadId,
    const AggregationBuildPhysicalOperator* buildOperator)
{
    PRECONDITION(operatorHandler != nullptr, "The operator handler should not be null");
    PRECONDITION(buildOperator != nullptr, "The build operator should not be null");

    /// If a new hashmap slice is created, we need to set the cleanup function for the aggregation states
    const CreateNewHashMapSliceArgs hashMapSliceArgs{
        {operatorHandler->cleanupStateNautilusFunction},
        buildOperator->hashMapOptions.keySize,
        buildOperator->hashMapOptions.valueSize,
        buildOperator->hashMapOptions.pageSize,
        buildOperator->hashMapOptions.numberOfBuckets};
    auto wrappedCreateFunction(
        [createFunction = operatorHandler->getCreateNewSlicesFunction(hashMapSliceArgs),
         cleanupStateNautilusFunction = operatorHandler->cleanupStateNautilusFunction](const SliceStart sliceStart, const SliceEnd sliceEnd)
        {
            const auto createdSlices = createFunction(sliceStart, sliceEnd);
            return createdSlices;
        });

    const auto hashMap = operatorHandler->getSliceAndWindowStore().getSlicesOrCreate(timestamp, wrappedCreateFunction);
    INVARIANT(
        hashMap.size() == 1,
        "We expect exactly one slice for the given timestamp during the AggregationBuild, as we currently solely support "
        "slicing, but got {}",
        hashMap.size());

    /// Converting the slice to an AggregationSlice and returning the pointer to the hashmap
    const auto aggregationSlice = std::dynamic_pointer_cast<AggregationSlice>(hashMap[0]);
    INVARIANT(aggregationSlice != nullptr, "The slice should be an AggregationSlice in an AggregationBuild");
    return aggregationSlice->getHashMapPtrOrCreate(workerThreadId);
}

void AggregationBuildPhysicalOperator::setup(ExecutionContext& executionCtx, CompilationContext& compilationContext) const
{
    WindowBuildPhysicalOperator::setup(executionCtx, compilationContext);

    /// Creating the cleanup function for the slice of current stream
    /// As the setup function does not get traced, we do not need to have any nautilus::invoke calls to jump to the C++ runtime
    /// We are not allowed to use const or const references for the lambda function params, as nautilus does not support this in the registerFunction method.
    /// ReSharper disable once CppPassValueParameterByConstReference
    /// NOLINTBEGIN(performance-unnecessary-value-param)
    auto* const operatorHandler = dynamic_cast<AggregationOperatorHandler*>(executionCtx.getGlobalOperatorHandler(operatorHandlerId).value);
    if (bool expectedValue = false; operatorHandler->setupAlreadyCalled.compare_exchange_strong(expectedValue, true))
    {
        operatorHandler->cleanupStateNautilusFunction
            = std::make_shared<CreateNewHashMapSliceArgs::NautilusCleanupExec>(compilationContext.registerFunction(std::function(
                [copyOfHashMapOptions = hashMapOptions,
                 copyOfAggregationFunctions = aggregationPhysicalFunctions](nautilus::val<Nautilus::Interface::HashMap*> hashMap)
                {
                    const Interface::ChainedHashMapRef hashMapRef(
                        hashMap,
                        copyOfHashMapOptions.fieldKeys,
                        copyOfHashMapOptions.fieldValues,
                        copyOfHashMapOptions.entriesPerPage,
                        copyOfHashMapOptions.entrySize);
                    for (const auto entry : hashMapRef)
                    {
                        const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                            entry, hashMap, copyOfHashMapOptions.fieldKeys, copyOfHashMapOptions.fieldValues);
                        auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
                        for (const auto& aggFunction : nautilus::static_iterable(copyOfAggregationFunctions))
                        {
                            aggFunction->cleanup(state);
                            state = state + aggFunction->getSizeOfStateInBytes();
                        }
                    }
                })));
        if (auto* sessionHandler = dynamic_cast<SessionAggregationOperatorHandler*>(operatorHandler))
        {
            registerSessionFunctions(
                *sessionHandler, compilationContext, hashMapOptions, aggregationPhysicalFunctions, sessionHandler->getGapInMilliseconds());
        }
    }


    /// NOLINTEND(performance-unnecessary-value-param)
}

void AggregationBuildPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// Getting the operator handler from the local state
    auto* const localState = dynamic_cast<WindowOperatorBuildLocalState*>(ctx.getLocalState(id));
    auto operatorHandler = localState->getOperatorHandler();

    /// Getting the correspinding slice so that we can update the aggregation states
    const auto timestamp = timeFunction->getTs(ctx, record);
    const auto hashMapPtr = invoke(
        getAggHashMapProxy, operatorHandler, timestamp, ctx.workerThreadId, nautilus::val<const AggregationBuildPhysicalOperator*>(this));
    Interface::ChainedHashMapRef hashMap(
        hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues, hashMapOptions.entriesPerPage, hashMapOptions.entrySize);

    /// Calling the key functions to add/update the keys to the record
    for (nautilus::static_val<uint64_t> i = 0; i < hashMapOptions.fieldKeys.size(); ++i)
    {
        const auto& [fieldIdentifier, type, fieldOffset] = hashMapOptions.fieldKeys[i];
        const auto& function = hashMapOptions.keyFunctions[i];
        const auto value = function.execute(record, ctx.pipelineMemoryProvider.arena);
        record.write(fieldIdentifier, value);
    }

    /// Finding or creating the entry for the provided record
    const auto hashMapEntry = hashMap.findOrCreateEntry(
        record,
        *hashMapOptions.hashFunction,
        [&](const nautilus::val<Interface::AbstractHashMapEntry*>& entry)
        {
            /// If the entry for the provided keys does not exist, we need to create a new one and initialize the aggregation states
            const Interface::ChainedHashMapRef::ChainedEntryRef entryRefReset(
                entry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
            auto state = static_cast<nautilus::val<AggregationState*>>(entryRefReset.getValueMemArea());
            for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
            {
                aggFunction->reset(state, ctx.pipelineMemoryProvider);
                state = state + aggFunction->getSizeOfStateInBytes();
            }
        },
        ctx.pipelineMemoryProvider.bufferProvider);


    /// Updating the aggregation states
    const Interface::ChainedHashMapRef::ChainedEntryRef entryRef(
        hashMapEntry, hashMapPtr, hashMapOptions.fieldKeys, hashMapOptions.fieldValues);
    auto state = static_cast<nautilus::val<AggregationState*>>(entryRef.getValueMemArea());
    for (const auto& aggFunction : nautilus::static_iterable(aggregationPhysicalFunctions))
    {
        aggFunction->lift(state, ctx.pipelineMemoryProvider, record);
        state = state + aggFunction->getSizeOfStateInBytes();
    }
}

AggregationBuildPhysicalOperator::AggregationBuildPhysicalOperator(
    const OperatorHandlerId operatorHandlerId,
    std::unique_ptr<TimeFunction> timeFunction,
    std::vector<std::shared_ptr<AggregationPhysicalFunction>> aggregationFunctions,
    HashMapOptions hashMapOptions)
    : WindowBuildPhysicalOperator(operatorHandlerId, std::move(timeFunction))
    , aggregationPhysicalFunctions(std::move(aggregationFunctions))
    , hashMapOptions(std::move(hashMapOptions))
{
}

}
