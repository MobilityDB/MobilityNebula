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

#include <Aggregation/Function/Meos/TnumberExtentTransfnAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <cstring>
#include <cstdio>
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

static std::mutex meos_tnumberextent_mutex;


TnumberExtentTransfnAggregationPhysicalFunction::TnumberExtentTransfnAggregationPhysicalFunction(
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

void TnumberExtentTransfnAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    // Incremental accumulator slot: each event folds into the running value/time
    // TBox via tnumber_extent_transfn. O(1) state, no event buffer.
    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);
    auto value = valueValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, double valueVal, int64_t tsVal) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tnumberextent_mutex);
            TBox** slot = reinterpret_cast<TBox**>(st);
            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[64];
            snprintf(wkt, sizeof(wkt), "%.6f@%s", valueVal, ts.c_str());
            Temporal* inst = tfloat_in(wkt);
            if (!inst) {
                return;
            }
            *slot = tnumber_extent_transfn(*slot, inst);
            free(inst);
        },
        aggregationState, value, timestamp);
}

void TnumberExtentTransfnAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* s1, AggregationState* s2) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tnumberextent_mutex);
            TBox** slot1 = reinterpret_cast<TBox**>(s1);
            TBox** slot2 = reinterpret_cast<TBox**>(s2);
            if (!*slot2) {
                return;
            }
            if (!*slot1) {
                *slot1 = tbox_copy(*slot2);
                return;
            }
            TBox* merged = union_tbox_tbox(*slot1, *slot2, false);
            free(*slot1);
            *slot1 = merged;
        },
        aggregationState1, aggregationState2);
}

Nautilus::Record TnumberExtentTransfnAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();
    auto boxStr = nautilus::invoke(
        +[](AggregationState* st) -> char*
        {
            std::lock_guard<std::mutex> lock(meos_tnumberextent_mutex);
            TBox** slot = reinterpret_cast<TBox**>(st);
            if (!*slot) {
                return (char*) nullptr;
            }
            char* out = tbox_out(*slot, 15);
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
        {
            if (s) { memcpy(dest, s, len); free((void*) s); }
        },
        variableSized.getContent(), boxStr, boxLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void TnumberExtentTransfnAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        { TBox** slot = reinterpret_cast<TBox**>(st); *slot = nullptr; },
        aggregationState);
}

size_t TnumberExtentTransfnAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(TBox*);
}

void TnumberExtentTransfnAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        { TBox** slot = reinterpret_cast<TBox**>(st); if (*slot) { free(*slot); *slot = nullptr; } },
        aggregationState);
}

AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTnumberExtentTransfnAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TNUMBER_EXTENT_TRANSFN aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}

}
