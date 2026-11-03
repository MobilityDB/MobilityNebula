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

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <limits>
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
#include <tuple>
#include <vector>

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

// MEOS wrapper header + geo extension symbols for the trajectory and tpoint_length
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

// The (timestamp, lon, lat) events of one window, in arrival order.
using TrajectoryPoints = std::vector<std::tuple<uint64_t, double, double>>;


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

    // Gather the (timestamp, lon, lat) events of the window; the trajectory is built once
    // all of them are known, because events need not arrive in time order.
    auto pointsPtr = nautilus::invoke(+[]() -> void* { return new TrajectoryPoints(); });

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;

        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));

        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<uint64_t>>();

        nautilus::invoke(
            +[](void* points, double lonVal, double latVal, uint64_t tsVal) -> void
            { static_cast<TrajectoryPoints*>(points)->emplace_back(tsVal, lonVal, latVal); },
            pointsPtr,
            lon,
            lat,
            timestamp);
    }

    // Build the linear tgeogpoint sequence of the events in time order and return its
    // spheroidal length in metres. Events sharing one timestamp at one position are one
    // instant; events sharing one timestamp at different positions state no trajectory,
    // so the result is NaN.
    auto totalLength = nautilus::invoke(
        +[](void* pointsArg) -> double
        {
            const std::unique_ptr<TrajectoryPoints> points(static_cast<TrajectoryPoints*>(pointsArg));
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_length_mutex);

            std::stable_sort(points->begin(), points->end(),
                             [](const auto& a, const auto& b) { return std::get<0>(a) < std::get<0>(b); });

            std::vector<TInstant*> instants;
            instants.reserve(points->size());
            const auto freeInstants = [&instants]()
            {
                for (auto* inst : instants)
                {
                    free(inst);
                }
            };
            TimestampTz previous = 0;
            double previousLon = 0.0;
            double previousLat = 0.0;
            for (const auto& [tsVal, lonVal, latVal] : *points)
            {
                const TimestampTz t = timestamptz_in(MEOS::Meos::convertEpochToTimestamp(tsVal).c_str(), -1);
                if (!instants.empty() && t == previous)
                {
                    if (lonVal == previousLon && latVal == previousLat)
                    {
                        continue;
                    }
                    freeInstants();
                    return std::numeric_limits<double>::quiet_NaN();
                }
                GSERIALIZED* gs = geogpoint_make2d(4326, lonVal, latVal);
                instants.push_back(tpointinst_make(gs, t));
                free(gs);
                previous = t;
                previousLon = lonVal;
                previousLat = latVal;
            }

            double length = 0.0;
            if (instants.size() > 1)
            {
                TSequence* seq = tsequence_make(instants.data(), static_cast<int>(instants.size()), true, true, LINEAR, true);
                length = tpoint_length(reinterpret_cast<Temporal*>(seq), true);
                free(seq);
            }
            freeInstants();
            return length;
        },
        pointsPtr);

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
