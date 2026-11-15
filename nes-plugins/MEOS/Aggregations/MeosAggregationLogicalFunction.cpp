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

#include <Aggregations/MeosAggregationLogicalFunction.hpp>

#include <algorithm>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// The same type, whatever the nullability of either
bool sameType(const DataType& left, const DataType& right)
{
    return left.type == right.type and left.extension == right.extension;
}

/// Whether a constant of the query reads as a value of the type: a typed constant (TBOX('...')) of that type,
/// or the text of an untyped one: any text as a variable-sized value, a number as a float, an integer as an
/// integer, true or false as a boolean
bool readsAs(const std::string_view type, const LogicalFunction& literal)
{
    const auto constant = literal.tryGetAs<ConstantValueLogicalFunction>();
    if (not constant)
    {
        return false;
    }
    const auto literalType = (*constant)->getDataType();
    if (not literalType.isType(DataType::Type::UNDEFINED))
    {
        return sameType(literalType, DataTypeProvider::provideDataType(std::string(type)));
    }
    const auto text = (*constant)->getConstantValue();
    if (type == "VARSIZED")
    {
        return true;
    }
    if (type == "BOOLEAN")
    {
        return toUpperCase(text) == "TRUE" or toUpperCase(text) == "FALSE";
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

/// The data type of the field an aggregation reads, undefined while it is unbound
DataType fieldType(const AggregationFieldAccess& field)
{
    if (const auto* bound = std::get_if<TypedLogicalFunction<FieldAccessLogicalFunction>>(&field))
    {
        return (*bound)->getDataType();
    }
    return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED);
}
}

std::optional<MeosAggregate> meosAggregate(const std::string_view name)
{
    const auto aggregates = meosAggregates();
    const auto found = std::ranges::lower_bound(aggregates, name, {}, &MeosAggregate::name);
    if (found == aggregates.end() or found->name != name)
    {
        return std::nullopt;
    }
    return *found;
}

MeosAggregationLogicalFunction::MeosAggregationLogicalFunction(
    const std::string_view name,
    const std::span<const MeosAggregateOverload> overloads,
    AggregationFieldAccess inputFunction,
    std::vector<LogicalFunction> literals)
    : name(name), overloads(overloads), inputFunction(std::move(inputFunction)), literals(std::move(literals))
{
    /// An unreflected aggregation's field carries its inferred type already
    chosen = chooseOverload(fieldType(this->inputFunction));
}

std::optional<size_t> MeosAggregationLogicalFunction::chooseOverload(const DataType& type) const
{
    if (type.isType(DataType::Type::UNDEFINED))
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
            const bool same = sameType(type, argumentType);
            const bool castable = castNumbers and type.isNumeric() and argumentType.isNumeric();
            if ((same or castable) and literalsRead(overloads[i]))
            {
                return i;
            }
        }
    }
    return std::nullopt;
}

std::string_view MeosAggregationLogicalFunction::getName() const noexcept
{
    return chosen ? overloads[*chosen].physicalAggregation : name;
}

DataType MeosAggregationLogicalFunction::getAggregateType() const
{
    if (not chosen)
    {
        return DataTypeProvider::provideDataType(DataType::Type::UNDEFINED);
    }
    /// An aggregate over no value answers null, as in MobilityDB
    return DataTypeProvider::provideDataType(std::string(overloads[*chosen].result), DataType::NULLABLE::IS_NULLABLE);
}

MeosAggregationLogicalFunction MeosAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.inputFunction = inferFieldAccess(inputFunction, schema);
    copy.chosen = copy.chooseOverload(fieldType(copy.inputFunction));
    if (not copy.chosen)
    {
        throw UnsupportedQuery(
            "{} takes no argument of type {} followed by the constants ({})",
            name,
            fieldType(copy.inputFunction),
            fmt::join(literals | std::views::transform([](const auto& literal) { return literal.explain(ExplainVerbosity::Short); }), ", "));
    }
    return copy;
}

std::string MeosAggregationLogicalFunction::explain(const ExplainVerbosity verbosity) const
{
    auto input = std::visit([verbosity](const auto& field) { return field->explain(verbosity); }, inputFunction);
    if (literals.empty())
    {
        return fmt::format("{}({})", name, input);
    }
    return fmt::format(
        "{}({}, {})",
        name,
        input,
        fmt::join(literals | std::views::transform([verbosity](const auto& literal) { return literal.explain(verbosity); }), ", "));
}

bool MeosAggregationLogicalFunction::operator==(const MeosAggregationLogicalFunction& other) const
{
    return name == other.name and chosen == other.chosen and inputFunction == other.inputFunction and literals == other.literals;
}

Reflected Reflector<MeosAggregationLogicalFunction>::operator()(
    const MeosAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedMeosAggregationLogicalFunction{
        .name = std::string(function.getAggregateName()), .inputFunction = function.getInputFunction(), .literals = function.getLiterals()});
}

MeosAggregationLogicalFunction
Unreflector<MeosAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [name, inputFunction, literals] = context.unreflect<detail::ReflectedMeosAggregationLogicalFunction>(reflected);
    /// The name the aggregation keeps is the catalog's, of static storage as its overloads are
    const auto aggregate = meosAggregate(name);
    if (not aggregate)
    {
        throw CannotDeserialize("{} is not a MEOS aggregate", name);
    }
    return MeosAggregationLogicalFunction{aggregate->name, aggregate->overloads, std::move(inputFunction), std::move(literals)};
}

}

size_t std::hash<NES::MeosAggregationLogicalFunction>::operator()(const NES::MeosAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), aggregationFunction.getName());
}
