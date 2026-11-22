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

#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunctionV2.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/TemporalAggregationSerde.hpp>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

TemporalSequenceAggregationLogicalFunctionV2::TemporalSequenceAggregationLogicalFunctionV2(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          lonField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField,
          asField)
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
TemporalSequenceAggregationLogicalFunctionV2::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
{
    // Default alias to lon field; will be adjusted in inferStamp
    return std::make_shared<TemporalSequenceAggregationLogicalFunctionV2>(lonField, latField, timestampField, lonField);
}

std::string_view TemporalSequenceAggregationLogicalFunctionV2::getName() const noexcept
{
    return NAME;
}

void TemporalSequenceAggregationLogicalFunctionV2::inferStamp(const Schema& schema)
{
    // infer all
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = lonField;

    if (!lonField.getDataType().isNumeric() || !latField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {
        throw CannotInferSchema("TemporalSequenceAggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }

    // Update alias field fully qualified name and data type
    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
    inputStamp = onField.getDataType();
}

NES::SerializableAggregationFunction TemporalSequenceAggregationLogicalFunctionV2::serialize() const
{
    return TemporalAggregationSerde::serializeTemporalSequence(onField, latField, timestampField, asField);
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 4)
    {
        auto function = TemporalSequenceAggregationLogicalFunctionV2::create(arguments.fields[0], arguments.fields[1], arguments.fields[2]);
        // last field is alias
        // NOTE: base class has no setter; emulate by constructing a new instance with alias
        auto ptr = std::make_shared<TemporalSequenceAggregationLogicalFunctionV2>(arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]);
        return ptr;
    }
    throw CannotDeserialize(
        "TemporalSequenceAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}",
        arguments.fields.size());
}

} // namespace NES
