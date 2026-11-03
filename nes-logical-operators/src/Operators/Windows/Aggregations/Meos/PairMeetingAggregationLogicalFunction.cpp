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

#include <Operators/Windows/Aggregations/Meos/PairMeetingAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/TemporalAggregationSerde.hpp>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <fmt/format.h>

namespace NES
{

PairMeetingAggregationLogicalFunction::PairMeetingAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& vehicleIdField,
    const FieldAccessLogicalFunction& asField,
    double dMeetMetres)
    : WindowAggregationLogicalFunction(
          lonField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField,
          asField)
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
    , vehicleIdField(vehicleIdField)
    , dMeetMetres(dMeetMetres)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
PairMeetingAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& vehicleIdField,
    double dMeetMetres)
{
    return std::make_shared<PairMeetingAggregationLogicalFunction>(
        lonField, latField, timestampField, vehicleIdField, lonField, dMeetMetres);
}

std::string_view PairMeetingAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void PairMeetingAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    vehicleIdField = vehicleIdField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = lonField;

    if (!lonField.getDataType().isNumeric() || !latField.getDataType().isNumeric()
        || !timestampField.getDataType().isNumeric() || !vehicleIdField.getDataType().isNumeric())
    {
        throw CannotInferSchema("PairMeetingAggregationLogicalFunction: lon, lat, timestamp, and vehicle_id fields must be numeric.");
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

NES::SerializableAggregationFunction PairMeetingAggregationLogicalFunction::serialize() const
{
    return TemporalAggregationSerde::serializePackedAggregation(
        NAME, lonField, {latField, timestampField, vehicleIdField}, asField, {fmt::format("{}", dMeetMetres)});
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterPairMeetingAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 5 && arguments.literals.size() == 1)
    {
        double dMeetMetres;
        try
        {
            dMeetMetres = std::stod(arguments.literals[0]);
        }
        catch (const std::exception&)
        {
            throw CannotDeserialize("PairMeetingAggregationLogicalFunction dMeet literal is not a number: `{}`", arguments.literals[0]);
        }
        return std::make_shared<PairMeetingAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4], dMeetMetres);
    }
    throw CannotDeserialize(
        "PairMeetingAggregationLogicalFunction requires lon, lat, timestamp, vehicle_id, alias fields and the dMeet literal but got {} "
        "fields and {} literals",
        arguments.fields.size(),
        arguments.literals.size());
}

} // namespace NES
