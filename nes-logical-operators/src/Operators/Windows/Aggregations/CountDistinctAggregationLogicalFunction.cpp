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

#include <Operators/Windows/Aggregations/CountDistinctAggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

#include <utility>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
CountDistinctAggregationLogicalFunction::CountDistinctAggregationLogicalFunction(const FieldAccessLogicalFunction& field)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field)
{
}

CountDistinctAggregationLogicalFunction::CountDistinctAggregationLogicalFunction(
    const FieldAccessLogicalFunction& field, FieldAccessLogicalFunction asField)
    : WindowAggregationLogicalFunction(
          field.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          field,
          std::move(asField))
{
}

std::string_view CountDistinctAggregationLogicalFunction::getName() const noexcept
{
    return NAME;
}

void CountDistinctAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    /// The values of the input field are compared, so they keep the type of the field; the count is a UINT64
    onField = onField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    ///Set fully qualified name for the as Field
    const auto onFieldName = onField.getFieldName();
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
    inputStamp = onField.getDataType();
    finalAggregateStamp = DataTypeProvider::provideDataType(DataType::Type::UINT64);
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
}

SerializableAggregationFunction CountDistinctAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(NAME);

    auto onFieldFuc = SerializableFunction();
    onFieldFuc.CopyFrom(onField.serialize());

    auto asFieldFuc = SerializableFunction();
    asFieldFuc.CopyFrom(asField.serialize());

    serializedAggregationFunction.mutable_as_field()->CopyFrom(asFieldFuc);
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onFieldFuc);
    return serializedAggregationFunction;
}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::RegisterCountDistinctAggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.fields.size() != 2)
    {
        throw CannotDeserialize("CountDistinctAggregationLogicalFunction requires exactly two fields, but got {}", arguments.fields.size());
    }
    return std::make_shared<CountDistinctAggregationLogicalFunction>(arguments.fields[0], arguments.fields[1]);
}
}
