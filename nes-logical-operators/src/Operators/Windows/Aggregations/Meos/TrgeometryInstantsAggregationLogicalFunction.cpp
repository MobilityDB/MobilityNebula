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

#include <Operators/Windows/Aggregations/Meos/TrgeometryInstantsAggregationLogicalFunction.hpp>

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

TrgeometryInstantsAggregationLogicalFunction::TrgeometryInstantsAggregationLogicalFunction(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          xField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          xField,
          asField)
    , xField(xField)
    , yField(yField)
    , thetaField(thetaField)
    , timestampField(timestampField)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
TrgeometryInstantsAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField)
{
    return std::make_shared<TrgeometryInstantsAggregationLogicalFunction>(xField, yField, thetaField, timestampField, xField);
}

std::string_view TrgeometryInstantsAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void TrgeometryInstantsAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    xField = xField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    yField = yField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    thetaField = thetaField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = xField;

    if (!xField.getDataType().isNumeric() || !yField.getDataType().isNumeric()
        || !thetaField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {
        throw CannotInferSchema("TrgeometryInstantsAggregationLogicalFunction: x, y, theta, and timestamp fields must be numeric.");
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

NES::SerializableAggregationFunction TrgeometryInstantsAggregationLogicalFunction::serialize() const
{
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(xField, yField, thetaField, timestampField, asField);
    saf.set_type(std::string(NAME));
    return saf;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterTrgeometryInstantsAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 5)
    {
        auto ptr = std::make_shared<TrgeometryInstantsAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4]);
        return ptr;
    }
    throw CannotDeserialize(
        "TrgeometryInstantsAggregationLogicalFunction requires x, y, theta, timestamp, and alias fields but got {}",
        arguments.fields.size());
}

} // namespace NES
