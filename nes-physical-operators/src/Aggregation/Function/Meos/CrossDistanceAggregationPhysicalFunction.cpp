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
#include <limits>
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

        // Keep, for each target VID, the event with the greatest timestamp, whatever the
        // arrival order. vidA / vidB are passed through to the captureless lambda alongside
        // the state pointer (Nautilus invoke ABI forbids closures).
        nautilus::invoke(
            +[](double* scratch, double lonVal, double latVal, int64_t tsVal, uint64_t vid,
                uint64_t vidAArg, uint64_t vidBArg) -> void
            {
                const double ts = static_cast<double>(tsVal);
                if (vid == vidAArg && (std::isnan(scratch[2]) || ts >= scratch[2])) {
                    scratch[0] = lonVal;
                    scratch[1] = latVal;
                    scratch[2] = ts;
                } else if (vid == vidBArg && (std::isnan(scratch[5]) || ts >= scratch[5])) {
                    scratch[3] = lonVal;
                    scratch[4] = latVal;
                    scratch[5] = ts;
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

            // Spheroidal distance in metres between the two WGS84 positions.
            GSERIALIZED* gA = geogpoint_make2d(4326, scratch[0], scratch[1]);
            GSERIALIZED* gB = geogpoint_make2d(4326, scratch[3], scratch[4]);
            free(scratch);
            const double distance = geog_distance(gA, gB, true);
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
