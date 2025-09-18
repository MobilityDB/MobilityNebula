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

#include <Operators/Windows/Aggregations/Meos/TemporalSequenceAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{
// Single-field constructors removed - TEMPORAL_SEQUENCE requires three separate fields

TemporalSequenceAggregationLogicalFunction::TemporalSequenceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField)
    : WindowAggregationLogicalFunction(
          lonField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField),
      lonField(lonField), latField(latField), timestampField(timestampField)
{
}

// Single-field create methods removed - TEMPORAL_SEQUENCE requires three separate fields

std::shared_ptr<WindowAggregationLogicalFunction> TemporalSequenceAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField)
{
    return std::make_shared<TemporalSequenceAggregationLogicalFunction>(lonField, latField, timestampField);
}

std::string_view TemporalSequenceAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}
void TemporalSequenceAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// For TEMPORAL_SEQUENCE, we need to infer types for all three fields
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    
    /// We also update onField for backward compatibility
    onField = lonField;

    if (!lonField.getDataType().isNumeric() || !latField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {
        NES_FATAL_ERROR("TemporalSequenceAggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }

    ///Set fully qualified name for the as Field
    const auto onFieldName = lonField.getFieldName();
    const auto asFieldName = asField.getFieldName();

    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    ///If on and as field name are different then append the attribute name resolver from on field to the as field
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }
    else
    {
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }
    auto newAsField = asField.withDataType(getFinalAggregateStamp());
    asField = newAsField.get<FieldAccessLogicalFunction>();
    inputStamp = lonField.getDataType();
}

NES::SerializableAggregationFunction TemporalSequenceAggregationLogicalFunction::serialize() const
{
    NES::SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    SerializableFunction lonProto;
    lonProto.CopyFrom(lonField.serialize());
    serializedAggregationFunction.mutable_on_field()->CopyFrom(lonProto);

    SerializableFunction asProto;
    asProto.CopyFrom(asField.serialize());
    serializedAggregationFunction.mutable_as_field()->CopyFrom(asProto);

    SerializableFunction latProto;
    latProto.CopyFrom(latField.serialize());
    serializedAggregationFunction.add_extra_fields()->CopyFrom(latProto);

    SerializableFunction tsProto;
    tsProto.CopyFrom(timestampField.serialize());
    serializedAggregationFunction.add_extra_fields()->CopyFrom(tsProto);

    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterTemporalSequenceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 4)
    {
        auto function = TemporalSequenceAggregationLogicalFunction::create(arguments.fields[0], arguments.fields[1], arguments.fields[2]);
        function->setAsField(arguments.fields[3]);
        return function;
    }
    NES_FATAL_ERROR(
        "TemporalSequenceAggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {}",
        arguments.fields.size());
    return {};
}

} // namespace NES
