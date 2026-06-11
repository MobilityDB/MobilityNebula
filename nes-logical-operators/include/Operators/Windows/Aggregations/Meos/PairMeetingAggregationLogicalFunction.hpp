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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

/**
 * @brief Logical-plan side of the PAIR_MEETING aggregation (BerlinMOD-Q5).
 *
 * Four input fields (lon, lat, timestamp, vehicle_id). Final aggregate stamp = VARSIZED
 * (string-encoded list of meeting pairs). See `PairMeetingAggregationPhysicalFunction`
 * for the lift / combine / lower path.
 */
class PairMeetingAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField,
           const FieldAccessLogicalFunction& latField,
           const FieldAccessLogicalFunction& timestampField,
           const FieldAccessLogicalFunction& vehicleIdField);

    PairMeetingAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& vehicleIdField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~PairMeetingAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const noexcept { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const noexcept { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getVehicleIdField() const noexcept { return vehicleIdField; }

private:
    static constexpr std::string_view NAME = "PairMeeting";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;

    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
    FieldAccessLogicalFunction vehicleIdField;
};
}
