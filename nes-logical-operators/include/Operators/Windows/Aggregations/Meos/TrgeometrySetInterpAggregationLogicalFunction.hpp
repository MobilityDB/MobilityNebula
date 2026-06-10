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

#include <string>
#include <vector>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{

/**
 * @brief Windowed tpose -> trgeometry, with its interpolation set (trgeometry_set_interp) as hex-WKB.
 *
 * Four-input (x, y, theta, ts) tpose aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) pose sequence,
 * converts it to a trgeometry via `geo_tpose_to_trgeometry`, and applies the
 * MEOS sequence accessor `` to fold it.
 */
class TrgeometrySetInterpAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& xField, const FieldAccessLogicalFunction& yField, const FieldAccessLogicalFunction& thetaField, const FieldAccessLogicalFunction& timestampField, std::vector<std::string> constArgs);

    TrgeometrySetInterpAggregationLogicalFunction(
        const FieldAccessLogicalFunction& xField,
        const FieldAccessLogicalFunction& yField,
        const FieldAccessLogicalFunction& thetaField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField,
        std::vector<std::string> constArgs);

    [[nodiscard]] const std::vector<std::string>& getConstArgs() const noexcept { return constArgs; }

    void inferStamp(const Schema& schema) override;
    ~TrgeometrySetInterpAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getXField() const noexcept { return xField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getYField() const noexcept { return yField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getThetaField() const noexcept { return thetaField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }

private:
    static constexpr std::string_view NAME = "TrgeometrySetInterp";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;

    FieldAccessLogicalFunction xField;
    FieldAccessLogicalFunction yField;
    FieldAccessLogicalFunction thetaField;
    FieldAccessLogicalFunction timestampField;
    std::vector<std::string> constArgs;
};
}
