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

#include <cstdint>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

/**
 * @brief Logical-plan side of the CROSS_DISTANCE aggregation (BerlinMOD-Q9).
 *
 * Four input fields (lon, lat, timestamp, vehicle_id) + per-aggregation `(vidA, vidB)`
 * target-vehicle pair (the two integer constants identifying which vehicles to compute
 * the distance between). Final aggregate stamp = FLOAT64 (spheroidal distance in metres
 * between the two vehicles' latest known positions in the window; NaN if either is
 * unobserved). See `CrossDistanceAggregationPhysicalFunction`.
 *
 * @note The Registrar deserialize path receives only the 5 field args (lon, lat, ts,
 * vid, asField) and reconstructs the aggregation with the `DEFAULT_VID_A` /
 * `DEFAULT_VID_B` constants. Round-trip Serde fidelity for the vidA/vidB values is a
 * follow-up; mirrors PairMeeting #19's same Serde caveat (the proto carries only
 * SerializableFunction-typed fields in `extra_fields`).
 */
class CrossDistanceAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    /// BerlinMOD-scaffold defaults; mirror `CrossDistanceAggregationPhysicalFunction`.
    /// Used by the Registrar deserialize path; the parser path always supplies
    /// explicit values.
    static constexpr uint64_t DEFAULT_VID_A = 100;
    static constexpr uint64_t DEFAULT_VID_B = 200;

    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField,
           const FieldAccessLogicalFunction& latField,
           const FieldAccessLogicalFunction& timestampField,
           const FieldAccessLogicalFunction& vehicleIdField,
           uint64_t vidA,
           uint64_t vidB);

    CrossDistanceAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& vehicleIdField,
        const FieldAccessLogicalFunction& asField,
        uint64_t vidA,
        uint64_t vidB);

    void inferStamp(const Schema& schema) override;
    ~CrossDistanceAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const noexcept { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const noexcept { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getVehicleIdField() const noexcept { return vehicleIdField; }
    [[nodiscard]] uint64_t getVidA() const noexcept { return vidA; }
    [[nodiscard]] uint64_t getVidB() const noexcept { return vidB; }

private:
    static constexpr std::string_view NAME = "CrossDistance";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;

    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
    FieldAccessLogicalFunction vehicleIdField;
    uint64_t vidA;
    uint64_t vidB;
};
}
