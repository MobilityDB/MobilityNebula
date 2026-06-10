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
 * @brief Windowed GEO_UNION_TRANSFN -> unioned container (text) via geo_union_transfn/set_union_finalfn.
 *
 * Two-input (value, ts) tnumber aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) tnumber
 * sequence and calls MEOS `geo_union_transfn` to fold it to a single scalar.
 */
class GeoUnionTransfnAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& valueField, const FieldAccessLogicalFunction& timestampField);

    GeoUnionTransfnAggregationLogicalFunction(
        const FieldAccessLogicalFunction& valueField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~GeoUnionTransfnAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getValueField() const noexcept { return valueField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept { return timestampField; }

private:
    static constexpr std::string_view NAME = "GeoUnionTransfn";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;

    FieldAccessLogicalFunction valueField;
    FieldAccessLogicalFunction timestampField;
};
}
