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

#include <Aggregation/Function/Meos/PairMeetingAggregationPhysicalFunction.hpp>

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
#include <unordered_map>
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

static std::mutex pair_meeting_mutex;

PairMeetingAggregationPhysicalFunction::PairMeetingAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    PhysicalFunction vehicleIdFunctionParam,
    double dMeetMetres,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
    , vehicleIdFunction(std::move(vehicleIdFunctionParam))
    , dMeetMetres(dMeetMetres)
{
}

void PairMeetingAggregationPhysicalFunction::lift(
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

void PairMeetingAggregationPhysicalFunction::combine(
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

Nautilus::Record PairMeetingAggregationPhysicalFunction::lower(
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

    // Allocate an empty result buffer up-front; the lower step will fill it during the
    // single pass over the PagedVector entries.
    auto pairsBuffer = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {
            // Worst case: every vehicle pair could meet. Pre-allocate ~80 bytes per emitted
            // pair (BerlinMOD vehicle counts at the scaffold scale never exceed double digits
            // per window, so this is a safe upper bound).
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 80 + 64;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            return buffer;
        },
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {
        // Empty window — emit empty string
        auto emptyLen = nautilus::val<size_t>(0);
        auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(emptyLen);
        nautilus::invoke(+[](char* buffer) -> void { free(buffer); }, pairsBuffer);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, variableSized);
        return resultRecord;
    }

    // Walk every entry; the lambda maintains a per-vehicle latest-position map.
    // (Nautilus invoke ABI requires that all state be passed through pointer args; we
    // model the map as a plain std::unordered_map<uint64_t, std::tuple<...>> allocated
    // via new and threaded as a void* through the invoke calls.)
    auto vehicleMapPtr = nautilus::invoke(
        +[]() -> void*
        {
            return new std::unordered_map<uint64_t, std::tuple<double, double, int64_t>>();
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

        nautilus::invoke(
            +[](void* mapPtr, double lonVal, double latVal, int64_t tsVal, uint64_t vid) -> void
            {
                auto* map = static_cast<std::unordered_map<uint64_t, std::tuple<double, double, int64_t>>*>(mapPtr);
                // Overwrite-on-insert => map ends up holding the LATEST event per vehicle
                // (since the PagedVector iteration preserves insertion order).
                (*map)[vid] = std::make_tuple(lonVal, latVal, tsVal);
            },
            vehicleMapPtr, lon, lat, timestamp, vehicleId);
    }

    // Now enumerate pairs and check geog_dwithin(a, b, dMeet).
    // dMeet is passed in via the captureless lambda's arg list (Nautilus invoke ABI
    // forbids closures; we thread the threshold through alongside the state pointers).
    nautilus::invoke(
        +[](void* mapPtr, char* outBuffer, double dMeet) -> void
        {
            std::lock_guard<std::mutex> lock(pair_meeting_mutex);
            auto* map = static_cast<std::unordered_map<uint64_t, std::tuple<double, double, int64_t>>*>(mapPtr);

            // Stable iteration order
            std::vector<uint64_t> vids;
            vids.reserve(map->size());
            for (const auto& kv : *map)
            {
                vids.push_back(kv.first);
            }
            std::sort(vids.begin(), vids.end());

            bool first = true;
            for (size_t i = 0; i + 1 < vids.size(); ++i)
            {
                for (size_t j = i + 1; j < vids.size(); ++j)
                {
                    const auto& [lonA, latA, tsA] = (*map)[vids[i]];
                    const auto& [lonB, latB, tsB] = (*map)[vids[j]];

                    char wktA[80];
                    char wktB[80];
                    snprintf(wktA, sizeof(wktA), "SRID=4326;Point(%.7f %.7f)", lonA, latA);
                    snprintf(wktB, sizeof(wktB), "SRID=4326;Point(%.7f %.7f)", lonB, latB);
                    GSERIALIZED* gA = geom_in(wktA, -1);
                    GSERIALIZED* gB = geom_in(wktB, -1);
                    if (gA == nullptr || gB == nullptr) {
                        if (gA) free(gA);
                        if (gB) free(gB);
                        continue;
                    }
                    GSERIALIZED* ggA = geom_to_geog(gA);
                    GSERIALIZED* ggB = geom_to_geog(gB);
                    bool meets = geog_dwithin(ggA, ggB, dMeet, true);
                    if (meets) {
                        // Use the later of the two timestamps as the meeting time
                        int64_t tsMax = (tsA > tsB) ? tsA : tsB;
                        // Approximate distance via geog distance (not exposed in meos_geo here yet);
                        // emit (vidA, vidB, ts, "≤dMeet") triple
                        char buf[128];
                        snprintf(buf, sizeof(buf), "%s%lu,%lu,%lld,<=%.1f",
                                 first ? "" : ";",
                                 (unsigned long)vids[i], (unsigned long)vids[j],
                                 (long long)tsMax,
                                 dMeet);
                        strcat(outBuffer, buf);
                        first = false;
                    }
                    free(ggA);
                    free(ggB);
                    free(gA);
                    free(gB);
                }
            }
            delete map;
        },
        vehicleMapPtr, pairsBuffer, nautilus::val<double>(dMeetMetres));

    // Allocate VARSIZED output sized to the assembled string
    auto strLen = nautilus::invoke(
        +[](const char* buffer) -> size_t { return strlen(buffer); },
        pairsBuffer);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(strLen);
    nautilus::invoke(
        +[](int8_t* dest, const char* src, size_t len) -> void
        {
            if (len > 0) memcpy(dest, src, len);
            free((void*)src);
        },
        variableSized.getContent(), pairsBuffer, strLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void PairMeetingAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t PairMeetingAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void PairMeetingAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
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


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterPairMeetingAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("PAIR_MEETING aggregation cannot be created through the registry. "
                           "It requires four field functions (longitude, latitude, timestamp, vehicle_id)");
}

}
