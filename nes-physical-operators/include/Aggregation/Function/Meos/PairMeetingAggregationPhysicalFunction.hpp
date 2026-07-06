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

#pragma once

#include <cstddef>
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <val_concepts.hpp>

namespace NES
{

/**
 * @brief Cartesian aggregation that emits the BerlinMOD-Q5 pair-meeting answer per window.
 *
 * Takes four input fields: lon, lat, timestamp, vehicle_id, plus a per-aggregation
 * `dMeetMetres` distance threshold passed via the SQL constant arg
 * (`PAIR_MEETING(lon, lat, ts, vehicle_id, 200.0)`). The lift step stores per-event
 * tuples in a PagedVector. The lower step picks each vehicle's last-known position in the
 * window, enumerates vehicle pairs (a < b), and emits pairs whose spheroidal distance is
 * at most `dMeetMetres`. Result is a VARSIZED string `"vidA,vidB,ts,dist;..."` — same
 * shape pattern as TemporalSequence's BINARY(N) result.
 *
 * @note `DEFAULT_DMEET_METRES` (200 m) preserves the previous BerlinMOD-scaffold
 * default; used by the Registrar deserialize path until full Serde round-trip for the
 * dMeet constant is added (currently the proto carries only the 4 field + asField args
 * via `SerializableAggregationFunction.extra_fields`).
 *
 * Closes the MobilityNebula BerlinMOD-Q5 × 3-form partial→full gap; this PR makes the
 * meeting-distance configurable per-query.
 */
class PairMeetingAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    /// BerlinMOD-scaffold default (preserved when the SQL omits the constant arg via the
    /// Serde-deserialize path; the parser path always supplies an explicit value).
    static constexpr double DEFAULT_DMEET_METRES = 200.0;

    PairMeetingAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction lonFunctionParam,
        PhysicalFunction latFunctionParam,
        PhysicalFunction timestampFunctionParam,
        PhysicalFunction vehicleIdFunctionParam,
        double dMeetMetres,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record)
        override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~PairMeetingAggregationPhysicalFunction() override = default;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    PhysicalFunction lonFunction;
    PhysicalFunction latFunction;
    PhysicalFunction timestampFunction;
    PhysicalFunction vehicleIdFunction;
    double dMeetMetres;
};

}
