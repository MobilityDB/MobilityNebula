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

#include <Operators/Windows/Aggregations/Meos/MeosAggregationLogicalFunction.hpp>

#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

namespace
{
/// Whether the text of a query literal reads as a value of the type: any text as a variable-sized value,
/// a number as a float, an integer as an integer, true or false as a boolean
bool readsAs(const std::string_view type, const std::string& text)
{
    if (type == "VARSIZED")
    {
        return true;
    }
    if (type == "BOOLEAN")
    {
        return Util::toUpperCase(text) == "TRUE" or Util::toUpperCase(text) == "FALSE";
    }
    const char* end = text.data() + text.size();
    if (type == "FLOAT64")
    {
        double value = 0;
        const auto [at, error] = std::from_chars(text.data(), end, value);
        return error == std::errc{} and at == end;
    }
    int64_t value = 0;
    const auto [at, error] = std::from_chars(text.data(), end, value);
    return error == std::errc{} and at == end;
}
}

MeosAggregationLogicalFunction::MeosAggregationLogicalFunction(
    const std::string_view name,
    const std::span<const MeosAggregateOverload> overloads,
    const FieldAccessLogicalFunction& onField,
    const FieldAccessLogicalFunction& asField,
    std::vector<std::string> literals)
    : WindowAggregationLogicalFunction(onField.getDataType(), onField.getDataType(), onField.getDataType(), onField, asField)
    , name(name)
    , overloads(overloads)
    , literals(std::move(literals))
{
    /// A deserialized aggregate's field carries its inferred type already
    chosen = chooseOverload();
    if (chosen)
    {
        finalAggregateStamp = DataTypeProvider::provideDataType(std::string(overloads[*chosen].result));
        partialAggregateStamp = finalAggregateStamp;
    }
}

std::optional<size_t> MeosAggregationLogicalFunction::chooseOverload() const
{
    const auto fieldType = onField.getDataType();
    if (fieldType.isType(DataType::Type::UNDEFINED))
    {
        return std::nullopt;
    }
    const auto literalsRead = [this](const MeosAggregateOverload& overload)
    {
        if (overload.literals.size() != literals.size())
        {
            return false;
        }
        for (size_t i = 0; i < literals.size(); ++i)
        {
            if (not readsAs(overload.literals[i], literals[i]))
            {
                return false;
            }
        }
        return true;
    };
    for (const bool castNumbers : {false, true})
    {
        for (size_t i = 0; i < overloads.size(); ++i)
        {
            const auto argumentType = DataTypeProvider::provideDataType(std::string(overloads[i].argument));
            if ((fieldType == argumentType or (castNumbers and fieldType.isNumeric() and argumentType.isNumeric()))
                and literalsRead(overloads[i]))
            {
                return i;
            }
        }
    }
    return std::nullopt;
}

std::vector<std::string> MeosAggregationLogicalFunction::getLiterals() const
{
    return literals;
}

std::string_view MeosAggregationLogicalFunction::getName() const noexcept
{
    return name;
}

std::string_view MeosAggregationLogicalFunction::getPhysicalName() const noexcept
{
    return chosen ? overloads[*chosen].physicalAggregation : name;
}

void MeosAggregationLogicalFunction::inferStamp(const Schema& schema)
{
    onField = onField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    chosen = chooseOverload();
    if (not chosen)
    {
        throw CannotInferStamp(
            "{} takes no argument of type {} followed by the literals ({})", name, onField.getDataType(), fmt::join(literals, ", "));
    }

    /// The as field qualified as the on field is, as every aggregation qualifies it
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
    inputStamp = onField.getDataType();
    finalAggregateStamp = DataTypeProvider::provideDataType(std::string(overloads[*chosen].result));
    partialAggregateStamp = finalAggregateStamp;
    asField = asField.withDataType(finalAggregateStamp).get<FieldAccessLogicalFunction>();
}

SerializableAggregationFunction MeosAggregationLogicalFunction::serialize() const
{
    SerializableAggregationFunction serializedAggregationFunction;
    serializedAggregationFunction.set_type(std::string(name));
    serializedAggregationFunction.mutable_on_field()->CopyFrom(onField.serialize());
    serializedAggregationFunction.mutable_as_field()->CopyFrom(asField.serialize());
    for (const auto& literal : literals)
    {
        serializedAggregationFunction.add_literals(literal);
    }
    return serializedAggregationFunction;
}

}
