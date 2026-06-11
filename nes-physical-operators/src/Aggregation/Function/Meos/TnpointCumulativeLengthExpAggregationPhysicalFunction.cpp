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

#include <Aggregation/Function/Meos/TnpointCumulativeLengthExpAggregationPhysicalFunction.hpp>

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
#include <meos_npoint.h>
}

namespace NES
{

static std::mutex meos_tnpointcumulativelengthexp_mutex;


TnpointCumulativeLengthExpAggregationPhysicalFunction::TnpointCumulativeLengthExpAggregationPhysicalFunction(
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

void TnpointCumulativeLengthExpAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    auto ridValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto fracValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto rid = ridValue.cast<nautilus::val<int64_t>>();
    auto frac = fracValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, int64_t ridVal, double fracVal, int64_t tsVal) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tnpointcumulativelengthexp_mutex);
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[80];
            snprintf(wkt, sizeof(wkt), "NPoint(%lld,%.6f)@%s", (long long) ridVal, fracVal, ts.c_str());

            Temporal* instTemp = tnpoint_in(wkt);
            if (!instTemp) {
                return;
            }
            if (*slot == nullptr) {
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            } else {
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }
            free(instTemp);
        },
        aggregationState,
        rid,
        frac,
        timestamp);
}

void TnpointCumulativeLengthExpAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* st1, AggregationState* st2) -> void
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tnpointcumulativelengthexp_mutex);
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

Nautilus::Record TnpointCumulativeLengthExpAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();

    auto hexStr = nautilus::invoke(
        +[](AggregationState* st) -> char*
        {
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_tnpointcumulativelengthexp_mutex);
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot == nullptr) {
                return (char*)nullptr;
            }
            Temporal* res = tnpoint_cumulative_length(*slot);
            if (!res) {
                return (char*)nullptr;
            }
            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(res, 0, &hexSize);
            free(res);
            return hexOut;
        },
        aggregationState);

    const auto hexLen = nautilus::invoke(
        +[](const char* s) -> size_t { return s ? strlen(s) : (size_t) 0; },
        hexStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(hexLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {
            if (s) {
                memcpy(dest, s, len);
                free((void*)s);
            }
        },
        variableSized.getContent(),
        hexStr,
        hexLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void TnpointCumulativeLengthExpAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        {
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            *slot = nullptr;
        },
        aggregationState);
}

size_t TnpointCumulativeLengthExpAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Temporal*);
}

void TnpointCumulativeLengthExpAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
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


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTnpointCumulativeLengthExpAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TNPOINT_CUMULATIVE_LENGTH_EXP aggregation cannot be created through the registry. "
                             "It requires three field functions (longitude, latitude, timestamp)");
}

} // namespace NES
