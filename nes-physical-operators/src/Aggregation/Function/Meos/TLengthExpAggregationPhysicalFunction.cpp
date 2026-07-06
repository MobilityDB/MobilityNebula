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

#include <Aggregation/Function/Meos/TLengthExpAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <mutex>
#include <cstring>
#include <cstdio>
#include <string>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
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
#include <meos_geo.h>
}

namespace NES
{

static std::mutex meos_tlengthexp_mutex;


TLengthExpAggregationPhysicalFunction::TLengthExpAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void TLengthExpAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto lon = lonValue.cast<nautilus::val<double>>();
    auto lat = latValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, double lonVal, double latVal, int64_t tsVal) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tlengthexp_mutex);
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[120];
            snprintf(wkt, sizeof(wkt), "SRID=4326;Point(%.6f %.6f)@%s", lonVal, latVal, ts.c_str());

            // Public instant constructor: a single-instant tgeompoint Temporal.
            Temporal* instTemp = tgeompoint_in(wkt);
            if (!instTemp) {
                return;
            }
            if (*slot == nullptr) {
                // First event: a 1-instant sequence; subsequent appendInstant calls
                // grow it in place (expand=true doubles maxcount when full).
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            } else {
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }
            free(instTemp);  // copied by tsequence_make / temporal_append_tinstant
        },
        aggregationState,
        lon,
        lat,
        timestamp);
}

void TLengthExpAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* st1, AggregationState* st2) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tlengthexp_mutex);
            Temporal** s1 = reinterpret_cast<Temporal**>(st1);
            Temporal** s2 = reinterpret_cast<Temporal**>(st2);
            if (*s2 == nullptr) {
                return;
            }
            if (*s1 == nullptr) {
                *s1 = *s2;
                *s2 = nullptr;
                return;
            }
            // temporal_merge returns a fresh temporal (copies inputs, frees nothing).
            Temporal* merged = temporal_merge(*s1, *s2);
            free(*s1);
            free(*s2);
            *s2 = nullptr;
            *s1 = merged;
        },
        aggregationState1,
        aggregationState2);
}

Nautilus::Record TLengthExpAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();

    auto resultValue = nautilus::invoke(
        +[](AggregationState* st) -> double
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tlengthexp_mutex);
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot == nullptr) {
                return (double)0;
            }
            return tpoint_length(*slot);
        },
        aggregationState);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;
}

void TLengthExpAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        {
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            *slot = nullptr;
        },
        aggregationState);
}

size_t TLengthExpAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Temporal*);
}

void TLengthExpAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        {
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot != nullptr) {
                free(*slot);
                *slot = nullptr;
            }
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTLengthExpAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TLENGTH_EXP aggregation cannot be created through the registry. "
                             "It requires three field functions (longitude, latitude, timestamp)");
}

} // namespace NES
