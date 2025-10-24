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

class TemporalSequenceAggregationLogicalFunction : public WindowAggregationLogicalFunction
{
public:
    // TEMPORAL_SEQUENCE requires three fields: longitude, latitude, and timestamp
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);
    TemporalSequenceAggregationLogicalFunction(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);

    void inferStamp(const Schema& schema) override;
    ~TemporalSequenceAggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const override { return true; }

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const { return lonField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const { return latField; }
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const { return timestampField; }

private:
    static constexpr std::string_view NAME = "TemporalSequence";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
    
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
};
}
