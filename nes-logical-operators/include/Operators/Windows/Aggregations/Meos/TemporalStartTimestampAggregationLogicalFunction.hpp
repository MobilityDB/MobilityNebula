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
 * @brief TimestampTz (MEOS μs-since-2000) of the first instant in the per-(window, group) tgeo trajectory.
 *
 * Three-input (lon, lat, ts) tgeo aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) trajectory
 * and calls MEOS `temporal_start_timestamptz` to fold it to a single scalar.
 */
class TemporalStartTimestampAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);

    TemporalStartTimestampAggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~TemporalStartTimestampAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const noexcept { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const noexcept { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }

private:
    static constexpr std::string_view NAME = "TemporalStartTimestamp";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::INT64;

    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};
}
