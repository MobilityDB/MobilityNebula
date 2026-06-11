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

#include <memory>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
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
    SerializableAggregationFunction saf;
    saf.set_type(std::string(NAME));

    SerializableFunction lonProto;
    lonProto.CopyFrom(LogicalFunction(lonField).serialize());
    saf.mutable_on_field()->CopyFrom(lonProto);

    SerializableFunction asProto;
    asProto.CopyFrom(LogicalFunction(asField).serialize());
    saf.mutable_as_field()->CopyFrom(asProto);

    SerializableFunction latProto;
    latProto.CopyFrom(LogicalFunction(latField).serialize());
    saf.add_extra_fields()->CopyFrom(latProto);

    SerializableFunction tsProto;
    tsProto.CopyFrom(LogicalFunction(timestampField).serialize());
    saf.add_extra_fields()->CopyFrom(tsProto);

    SerializableFunction vidProto;
    vidProto.CopyFrom(LogicalFunction(vehicleIdField).serialize());
    saf.add_extra_fields()->CopyFrom(vidProto);

    return saf;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCrossDistanceAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() == 5)
    {
        // The Registrar only carries the 5 field args (lon, lat, ts, vid, asField) — the
        // SerializableAggregationFunction proto does not yet have slots for the (vidA,
        // vidB) constants, so the deserialize path reconstructs with the
        // BerlinMOD-scaffold defaults. The parser path always supplies explicit values
        // from the SQL constant args. Adding (vidA, vidB) to the proto + extending the
        // Registrar args struct would close the round-trip gap; tracked as a follow-up
        // alongside the matching PairMeeting Serde follow-up (PR #19).
        auto ptr = std::make_shared<CrossDistanceAggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4],
            CrossDistanceAggregationLogicalFunction::DEFAULT_VID_A,
            CrossDistanceAggregationLogicalFunction::DEFAULT_VID_B);
        return ptr;
    }
    throw CannotDeserialize(
        "CrossDistanceAggregationLogicalFunction requires lon, lat, timestamp, vehicle_id, and alias fields but got {}",
        arguments.fields.size());
}

} // namespace NES
