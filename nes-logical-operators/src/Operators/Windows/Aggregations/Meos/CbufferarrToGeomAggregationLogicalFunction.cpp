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

#include <Operators/Windows/Aggregations/Meos/CbufferarrToGeomAggregationLogicalFunction.hpp>

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

CbufferarrToGeomAggregationLogicalFunction::CbufferarrToGeomAggregationLogicalFunction(
    const FieldAccessLogicalFunction& valueField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          valueField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          valueField,
          asField)
    , valueField(valueField)
    , timestampField(timestampField)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
CbufferarrToGeomAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& valueField,
    const FieldAccessLogicalFunction& timestampField)
{
    return std::make_shared<CbufferarrToGeomAggregationLogicalFunction>(valueField, timestampField, valueField);
}

std::string_view CbufferarrToGeomAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void CbufferarrToGeomAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    valueField = valueField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = valueField;

    if (!timestampField.getDataType().isNumeric())
    {
        throw CannotInferSchema("CbufferarrToGeomAggregationLogicalFunction: value must be a cbuffer literal; timestamp must be numeric.");
    }

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

NES::SerializableAggregationFunction CbufferarrToGeomAggregationLogicalFunction::serialize() const
{
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(valueField, timestampField, valueField, asField);
    saf.set_type(std::string(NAME));
    return saf;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCbufferarrToGeomAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    // serializeTemporalSequence only has a 4-field (lon, lat, ts, as) form, so
    // the two-field (value, ts) shape packs the value field twice; fields[2] is
    // that duplicate and is ignored here — the alias is fields[3].
    if (arguments.fields.size() == 4)
    {
        auto ptr = std::make_shared<CbufferarrToGeomAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[3]);
        return ptr;
    }
    throw CannotDeserialize(
        "CbufferarrToGeomAggregationLogicalFunction requires value, timestamp, and alias fields but got {}",
        arguments.fields.size());
}

} // namespace NES
