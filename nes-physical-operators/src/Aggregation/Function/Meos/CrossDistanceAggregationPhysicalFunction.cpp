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

#include <Aggregation/Function/Meos/CrossDistanceAggregationPhysicalFunction.hpp>

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <memory>
#include <mutex>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>

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
#include <meos_geo.h>
}

namespace NES
{

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";
constexpr static std::string_view VehicleIdFieldName = "vehicle_id";

static std::mutex cross_distance_mutex;

CrossDistanceAggregationPhysicalFunction::CrossDistanceAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    PhysicalFunction vehicleIdFunctionParam,
    uint64_t vidA,
    uint64_t vidB,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
    , vehicleIdFunction(std::move(vehicleIdFunctionParam))
    , vidA(vidA)
    , vidB(vidB)
{
}

void CrossDistanceAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);
    auto vehicleIdValue = vehicleIdFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({
        {std::string(LonFieldName), lonValue},
        {std::string(LatFieldName), latValue},
        {std::string(TimestampFieldName), timestampValue},
        {std::string(VehicleIdFieldName), vehicleIdValue}
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void CrossDistanceAggregationPhysicalFunction::combine(
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

Nautilus::Record CrossDistanceAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider)
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

    if (numberOfEntries == nautilus::val<size_t>(0)) {
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<double>(std::numeric_limits<double>::quiet_NaN()));
        return resultRecord;
    }

    // Allocate a 6-double scratch buffer on the heap (we cannot put std::optional<…> structures
    // through the nautilus invoke ABI). Layout: [lonA, latA, tsA, lonB, latB, tsB].
    // Sentinel ts = -1 means "not yet observed".
    auto scratchPtr = nautilus::invoke(
        +[]() -> double*
        {
            double* scratch = (double*)malloc(sizeof(double) * 6);
            // Bit-cast tsA, tsB sentinels by writing -1 as the int64 reinterpret of the double.
            // We just set them to NaN markers and treat NaN as "not observed".
            scratch[0] = std::numeric_limits<double>::quiet_NaN();
            scratch[1] = std::numeric_limits<double>::quiet_NaN();
            scratch[2] = std::numeric_limits<double>::quiet_NaN();
            scratch[3] = std::numeric_limits<double>::quiet_NaN();
            scratch[4] = std::numeric_limits<double>::quiet_NaN();
            scratch[5] = std::numeric_limits<double>::quiet_NaN();
            return scratch;
        });

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;

        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));
        const auto vehicleIdValue = itemRecord.read(std::string(VehicleIdFieldName));

        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();
        auto vehicleId = vehicleIdValue.cast<nautilus::val<uint64_t>>();

        // Overwrite-on-match — final value is the latest event for each target VID in iter order.
        // vidA / vidB are passed through to the captureless lambda alongside the state
        // pointer (Nautilus invoke ABI forbids closures); same pattern as
        // PairMeetingAggregationPhysicalFunction's dMeet threading in PR #19.
        nautilus::invoke(
            +[](double* scratch, double lonVal, double latVal, int64_t tsVal, uint64_t vid,
                uint64_t vidAArg, uint64_t vidBArg) -> void
            {
                if (vid == vidAArg) {
                    scratch[0] = lonVal;
                    scratch[1] = latVal;
                    scratch[2] = static_cast<double>(tsVal);
                } else if (vid == vidBArg) {
                    scratch[3] = lonVal;
                    scratch[4] = latVal;
                    scratch[5] = static_cast<double>(tsVal);
                }
            },
            scratchPtr, lon, lat, timestamp, vehicleId,
            nautilus::val<uint64_t>(vidA), nautilus::val<uint64_t>(vidB));
    }

    auto distanceMetres = nautilus::invoke(
        +[](double* scratch) -> double
        {
            // If either target vehicle has no observation in the window, return NaN.
            if (std::isnan(scratch[2]) || std::isnan(scratch[5])) {
                free(scratch);
                return std::numeric_limits<double>::quiet_NaN();
            }

            std::lock_guard<std::mutex> lock(cross_distance_mutex);

            char wktA[80];
            char wktB[80];
            snprintf(wktA, sizeof(wktA), "SRID=4326;Point(%.7f %.7f)", scratch[0], scratch[1]);
            snprintf(wktB, sizeof(wktB), "SRID=4326;Point(%.7f %.7f)", scratch[3], scratch[4]);
            free(scratch);

            GSERIALIZED* gA = geom_in(wktA, -1);
            GSERIALIZED* gB = geom_in(wktB, -1);
            if (gA == nullptr || gB == nullptr) {
                if (gA) free(gA);
                if (gB) free(gB);
                return std::numeric_limits<double>::quiet_NaN();
            }
            GSERIALIZED* ggA = geom_to_geog(gA);
            GSERIALIZED* ggB = geom_to_geog(gB);

            // For the spheroidal distance, dwithin probes only give boolean output; we want a
            // numeric value. The PROJ/MEOS shared object exposes `geog_distance` for this; here
            // we instead drive the MEOS NAD over single-instant tgeompoints which goes through
            // the same geog_distance path internally.
            char tgeoA[120];
            char tgeoB[120];
            snprintf(tgeoA, sizeof(tgeoA), "Point(%.7f %.7f)@2000-01-01 00:00:00", scratch[0], scratch[1]);
            snprintf(tgeoB, sizeof(tgeoB), "Point(%.7f %.7f)@2000-01-01 00:00:00", scratch[3], scratch[4]);
            Temporal* tA = (Temporal*)MEOS::Meos::parseTemporalPoint(std::string(tgeoA));
            Temporal* tB = (Temporal*)MEOS::Meos::parseTemporalPoint(std::string(tgeoB));
            double distance = std::numeric_limits<double>::quiet_NaN();
            if (tA != nullptr && tB != nullptr) {
                distance = nad_tgeo_tgeo(tA, tB);
            }
            if (tA != nullptr) MEOS::Meos::freeTemporalObject(tA);
            if (tB != nullptr) MEOS::Meos::freeTemporalObject(tB);
            free(ggA);
            free(ggB);
            free(gA);
            free(gB);
            return distance;
        },
        scratchPtr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, distanceMetres);
    return resultRecord;
}

void CrossDistanceAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t CrossDistanceAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void CrossDistanceAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
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


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterCrossDistanceAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("CROSS_DISTANCE aggregation cannot be created through the registry. "
                           "It requires four field functions (longitude, latitude, timestamp, vehicle_id)");
}

}
