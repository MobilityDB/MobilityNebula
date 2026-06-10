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

#include <Aggregation/Function/Meos/BigintUnionTransfnAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <mutex>
#include <cstring>
#include <string>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

#include <MEOSWrapper.hpp>
extern "C" {
#include <meos.h>
}

namespace NES
{

constexpr static std::string_view ValueFieldName = "value";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex meos_bigintunion_mutex;


BigintUnionTransfnAggregationPhysicalFunction::BigintUnionTransfnAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction valueFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), valueFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , valueFunction(std::move(valueFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void BigintUnionTransfnAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    // Incremental Set accumulator slot: each event folds its value into the
    // running set; O(1)-amortized state, no event buffer.
    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto value = valueValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, int64_t val) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_bigintunion_mutex);
            Set** slot = reinterpret_cast<Set**>(st);
            *slot = bigint_union_transfn(*slot, val);
        },
        aggregationState, value);
}

void BigintUnionTransfnAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* s1, AggregationState* s2) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_bigintunion_mutex);
            Set** slot1 = reinterpret_cast<Set**>(s1);
            Set** slot2 = reinterpret_cast<Set**>(s2);
            if (!*slot2) { return; }
            // set_union_transfn appends slot2's values into slot1 (creates from
            // slot2 if slot1 is null); slot2 is freed by its own cleanup.
            *slot1 = set_union_transfn(*slot1, *slot2);
        },
        aggregationState1, aggregationState2);
}

Nautilus::Record BigintUnionTransfnAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();
    auto setStr = nautilus::invoke(
        +[](AggregationState* st) -> char*
        {
            std::lock_guard<std::mutex> lock(meos_bigintunion_mutex);
            Set** slot = reinterpret_cast<Set**>(st);
            if (!*slot) { return (char*) nullptr; }
            // set_union_finalfn consumes (frees) the state and returns the
            // deduplicated, sorted Set; the slot must not be freed again.
            Set* fin = set_union_finalfn(*slot);
            *slot = nullptr;
            if (!fin) { return (char*) nullptr; }
            char* out = bigintset_out(fin);
            free(fin);
            return out;
        },
        aggregationState);

    const auto setLen = nautilus::invoke(
        +[](const char* s) -> size_t { return s ? strlen(s) : (size_t) 0; }, setStr);
    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(setLen);
    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        { if (s) { memcpy(dest, s, len); free((void*) s); } },
        variableSized.getContent(), setStr, setLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void BigintUnionTransfnAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(+[](AggregationState* st) -> void
        { Set** slot = reinterpret_cast<Set**>(st); *slot = nullptr; }, aggregationState);
}

size_t BigintUnionTransfnAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Set*);
}

void BigintUnionTransfnAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(+[](AggregationState* st) -> void
        { Set** slot = reinterpret_cast<Set**>(st); if (*slot) { free(*slot); *slot = nullptr; } }, aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterBigintUnionTransfnAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("BIGINT_UNION_TRANSFN aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}

}
