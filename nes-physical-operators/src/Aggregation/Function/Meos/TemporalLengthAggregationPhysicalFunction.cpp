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

#include <Aggregation/Function/Meos/TemporalLengthAggregationPhysicalFunction.hpp>

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

// MEOS wrapper header + geo extension symbols for tpoint_length
#include <MEOSWrapper.hpp>
extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace NES
{

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

// Mutex for thread-safe MEOS operations
static std::mutex meos_length_mutex;


TemporalLengthAggregationPhysicalFunction::TemporalLengthAggregationPhysicalFunction(
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

void TemporalLengthAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue}
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void TemporalLengthAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record TemporalLengthAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            return pagedVector->getTotalNumberOfEntries();
        },
        pagedVectorPtr);

    // Handle empty PagedVector case — zero-length trajectory
    if (numberOfEntries == nautilus::val<size_t>(0)) {
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<double>(0.0));
        return resultRecord;
    }

    // Build the trajectory string in the same MEOS instant-set format that
    // TemporalSequenceAggregationPhysicalFunction uses: {Point(lon lat)@ts, ...}
    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 150 + 50;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "{");
            return buffer;
        },
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;

        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));

        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

        trajectoryStr = nautilus::invoke(
            +[](char* buffer, double lonVal, double latVal, int64_t tsVal, int64_t counter) -> char*
            {
                if (counter > 0) {
                    strcat(buffer, ", ");
                }

                long long adjustedTime;
                if (tsVal > 1000000000000LL) {
                    adjustedTime = tsVal / 1000;
                } else {
                    adjustedTime = tsVal;
                }

                std::string timestampString = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                const char* timestampStr = timestampString.c_str();

                char pointStr[120];
                sprintf(pointStr, "Point(%.6f %.6f)@%s", lonVal, latVal, timestampStr);
                strcat(buffer, pointStr);
                return buffer;
            },
            trajectoryStr,
            lon,
            lat,
            timestamp,
            pointCounter);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }

    trajectoryStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {
            strcat(buffer, "}");
            return buffer;
        },
        trajectoryStr);

    // Parse the assembled trajectory into a MEOS Temporal object, call
    // tpoint_length on it, and free both the C string and the Temporal.
    auto totalLength = nautilus::invoke(
        +[](const char* trajStr) -> double
        {
            if (!trajStr || strlen(trajStr) == 0) {
                free((void*)trajStr);
                return 0.0;
            }

            std::lock_guard<std::mutex> lock(meos_length_mutex);

            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!temp) {
                free((void*)trajStr);
                return 0.0;
            }

            // tpoint_length is the MEOS C symbol from meos_geo.h. It returns the
            // spheroidal length in the SRID's distance unit (metres for the
            // BerlinMOD WGS84 trajectories that the scaffold streams).
            double length = tpoint_length(static_cast<Temporal*>(temp));

            MEOS::Meos::freeTemporalObject(temp);
            free((void*)trajStr);
            return length;
        },
        trajectoryStr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, totalLength);
    return resultRecord;
}

void TemporalLengthAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TemporalLengthAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void TemporalLengthAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(
                pagedVectorMemArea);
            pagedVector->~PagedVector();
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTemporalLengthAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TEMPORAL_LENGTH aggregation cannot be created through the registry. "
                           "It requires three field functions (longitude, latitude, timestamp)");
}

}
