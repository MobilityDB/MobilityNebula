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
#include <cstdint>
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <val_concepts.hpp>

namespace NES
{

/**
 * @brief Aggregation that emits the BerlinMOD-Q9 cross-distance between two specific
 * vehicles per window.
 *
 * Takes four input fields (lon, lat, timestamp, vehicle_id) plus a per-aggregation
 * `(vidA, vidB)` vehicle-pair passed via two SQL integer constant args
 * (`CROSS_DISTANCE(lon, lat, ts, vehicle_id, 100, 200)`). The lift step stores per-event
 * tuples; the lower step picks the latest known position of each target vehicle within
 * the window and emits the spheroidal `geog_distance(POINT, POINT)` between them as a
 * FLOAT64. Returns `NaN` when either target vehicle has no observation in the window.
 *
 * @note `DEFAULT_VID_A` (100) and `DEFAULT_VID_B` (200) preserve the previous
 * BerlinMOD-scaffold default; used by the Registrar deserialize path until full Serde
 * round-trip for the constant pair is added (currently the proto carries only the 4
 * field + asField args via `SerializableAggregationFunction.extra_fields`). Mirrors the
 * Serde caveat from PairMeeting #19.
 *
 * Closes the MobilityNebula BerlinMOD-Q9 × 3-form partial→full gap; this PR makes the
 * target vehicle pair configurable per-query.
 */
class CrossDistanceAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    /// BerlinMOD-scaffold defaults (preserved on the Serde-deserialize path; the parser
    /// path always supplies explicit values).
    static constexpr uint64_t DEFAULT_VID_A = 100;
    static constexpr uint64_t DEFAULT_VID_B = 200;

    CrossDistanceAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction lonFunctionParam,
        PhysicalFunction latFunctionParam,
        PhysicalFunction timestampFunctionParam,
        PhysicalFunction vehicleIdFunctionParam,
        uint64_t vidA,
        uint64_t vidB,
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
    ~CrossDistanceAggregationPhysicalFunction() override = default;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    PhysicalFunction lonFunction;
    PhysicalFunction latFunction;
    PhysicalFunction timestampFunction;
    PhysicalFunction vehicleIdFunction;
    uint64_t vidA;
    uint64_t vidB;
};

}
