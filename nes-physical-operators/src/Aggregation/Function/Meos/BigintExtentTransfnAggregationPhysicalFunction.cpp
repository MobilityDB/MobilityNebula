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

#include <Aggregation/Function/Meos/BigintExtentTransfnAggregationPhysicalFunction.hpp>

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

static std::mutex meos_bigintextent_mutex;


BigintExtentTransfnAggregationPhysicalFunction::BigintExtentTransfnAggregationPhysicalFunction(
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

void BigintExtentTransfnAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    // Incremental Span accumulator slot: each event folds into the running span;
    // O(1) state, no event buffer.
    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto value = valueValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, int64_t val) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_bigintextent_mutex);
            Span** slot = reinterpret_cast<Span**>(st);
            *slot = bigint_extent_transfn(*slot, val);
        },
        aggregationState, value);
}

void BigintExtentTransfnAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* s1, AggregationState* s2) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_bigintextent_mutex);
            Span** slot1 = reinterpret_cast<Span**>(s1);
            Span** slot2 = reinterpret_cast<Span**>(s2);
            if (!*slot2) { return; }
            if (!*slot1) { *slot1 = span_copy(*slot2); return; }
            Span* merged = super_union_span_span(*slot1, *slot2, false);
            free(*slot1);
            *slot1 = merged;
        },
        aggregationState1, aggregationState2);
}

Nautilus::Record BigintExtentTransfnAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();
    auto boxStr = nautilus::invoke(
        +[](AggregationState* st) -> char*
        {
            std::lock_guard<std::mutex> lock(meos_bigintextent_mutex);
            Span** slot = reinterpret_cast<Span**>(st);
            if (!*slot) { return (char*) nullptr; }
            char* out = bigintspan_out(*slot);
            free(*slot);
            *slot = nullptr;
            return out;
        },
        aggregationState);

    const auto boxLen = nautilus::invoke(
        +[](const char* s) -> size_t { return s ? strlen(s) : (size_t) 0; }, boxStr);
    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxLen);
    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        { if (s) { memcpy(dest, s, len); free((void*) s); } },
        variableSized.getContent(), boxStr, boxLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void BigintExtentTransfnAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(+[](AggregationState* st) -> void
        { Span** slot = reinterpret_cast<Span**>(st); *slot = nullptr; }, aggregationState);
}

size_t BigintExtentTransfnAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Span*);
}

void BigintExtentTransfnAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(+[](AggregationState* st) -> void
        { Span** slot = reinterpret_cast<Span**>(st); if (*slot) { free(*slot); *slot = nullptr; } }, aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterBigintExtentTransfnAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("BIGINT_EXTENT_TRANSFN aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}

}
