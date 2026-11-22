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

#include <Operators/Windows/Aggregations/Meos/CrossDistanceAggregationLogicalFunction.hpp>

#include <cstdint>
#include <exception>
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

CrossDistanceAggregationLogicalFunction::CrossDistanceAggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& vehicleIdField,
    const FieldAccessLogicalFunction& asField,
    uint64_t vidA,
    uint64_t vidB)
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
    , vidA(vidA)
    , vidB(vidB)
{
}

std::shared_ptr<WindowAggregationLogicalFunction>
CrossDistanceAggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& vehicleIdField,
    uint64_t vidA,
    uint64_t vidB)
{
    return std::make_shared<CrossDistanceAggregationLogicalFunction>(
        lonField, latField, timestampField, vehicleIdField, lonField, vidA, vidB);
}

std::string_view CrossDistanceAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void CrossDistanceAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    vehicleIdField = vehicleIdField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = lonField;

    if (!lonField.getDataType().isNumeric() || !latField.getDataType().isNumeric()
        || !timestampField.getDataType().isNumeric() || !vehicleIdField.getDataType().isNumeric())
    {
        throw CannotInferSchema("CrossDistanceAggregationLogicalFunction: lon, lat, timestamp, and vehicle_id fields must be numeric.");
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

NES::SerializableAggregationFunction CrossDistanceAggregationLogicalFunction::serialize() const
{
    return TemporalAggregationSerde::serializePackedAggregation(
        NAME, lonField, {latField, timestampField, vehicleIdField}, asField, {std::to_string(vidA), std::to_string(vidB)});
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCrossDistanceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 5 && arguments.literals.size() == 2)
    {
        uint64_t vidA;
        uint64_t vidB;
        try
        {
            vidA = std::stoull(arguments.literals[0]);
            vidB = std::stoull(arguments.literals[1]);
        }
        catch (const std::exception&)
        {
            throw CannotDeserialize(
                "CrossDistanceAggregationLogicalFunction (vidA, vidB) literals are not unsigned integers: `{}`, `{}`",
                arguments.literals[0],
                arguments.literals[1]);
        }
        return std::make_shared<CrossDistanceAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4], vidA, vidB);
    }
    throw CannotDeserialize(
        "CrossDistanceAggregationLogicalFunction requires lon, lat, timestamp, vehicle_id, alias fields and the (vidA, vidB) literals but "
        "got {} fields and {} literals",
        arguments.fields.size(),
        arguments.literals.size());
}

} // namespace NES
