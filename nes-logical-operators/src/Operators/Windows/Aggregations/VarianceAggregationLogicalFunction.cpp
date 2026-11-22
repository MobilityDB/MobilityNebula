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

#include <Operators/Windows/Aggregations/VarianceAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <folly/hash/Hash.h>
#include <magic_enum/magic_enum.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{
template <VarianceKind Kind>
VarianceAggregationLogicalFunction<Kind>::VarianceAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
}

template <VarianceKind Kind>
DataType VarianceAggregationLogicalFunction<Kind>::getAggregateType() const
{
    /// NULL where the variance is undefined, whatever the nullability of the input
    return DataTypeProvider::provideDataType(finalAggregateStampType, DataType::NULLABLE::IS_NULLABLE);
}

template <VarianceKind Kind>
bool VarianceAggregationLogicalFunction<Kind>::shallIncludeNullValues() noexcept
{
    return true;
}

template <VarianceKind Kind>
AggregationFieldAccess VarianceAggregationLogicalFunction<Kind>::getInputFunction() const
{
    return inputFunction;
}

template <VarianceKind Kind>
std::string_view VarianceAggregationLogicalFunction<Kind>::getName() noexcept
{
    return NAME;
}

template <VarianceKind Kind>
std::string VarianceAggregationLogicalFunction<Kind>::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

template <VarianceKind Kind>
bool VarianceAggregationLogicalFunction<Kind>::operator==(const VarianceAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

template <VarianceKind Kind>
VarianceAggregationLogicalFunction<Kind>
VarianceAggregationLogicalFunction<Kind>::withInferredType(const Schema<Field, Unordered>& schema) const
{
    const auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (!newInputFunction->getDataType().isNumeric())
    {
        throw UnsupportedQuery(
            "{} is only supported on numeric fields, but field {} has type {}",
            NAME,
            newInputFunction->getField().getLastName(),
            magic_enum::enum_name(newInputFunction->getDataType().type));
    }
    return VarianceAggregationLogicalFunction{newInputFunction};
}

template <VarianceKind Kind>
AggregationLogicalFunctionRegistryReturnType
VarianceAggregationLogicalFunction<Kind>::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("{} requires exactly one field, but got {}", NAME, arguments.on.size());
    }
    return VarianceAggregationLogicalFunction{arguments.on.at(0)};
}

template <VarianceKind Kind>
Reflected Reflector<VarianceAggregationLogicalFunction<Kind>>::operator()(
    const VarianceAggregationLogicalFunction<Kind>& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

template <VarianceKind Kind>
VarianceAggregationLogicalFunction<Kind>
Unreflector<VarianceAggregationLogicalFunction<Kind>>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return VarianceAggregationLogicalFunction<Kind>{context.unreflect<AggregationFieldAccess>(reflected)};
}

template class VarianceAggregationLogicalFunction<VarianceKind::Population>;
template class VarianceAggregationLogicalFunction<VarianceKind::Sample>;
template struct Reflector<VarianceAggregationLogicalFunction<VarianceKind::Population>>;
template struct Reflector<VarianceAggregationLogicalFunction<VarianceKind::Sample>>;
template struct Unreflector<VarianceAggregationLogicalFunction<VarianceKind::Population>>;
template struct Unreflector<VarianceAggregationLogicalFunction<VarianceKind::Sample>>;
}

template <NES::VarianceKind Kind>
size_t std::hash<NES::VarianceAggregationLogicalFunction<Kind>>::operator()(
    const NES::VarianceAggregationLogicalFunction<Kind>& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::VarianceAggregationLogicalFunction<Kind>::getName());
}

template struct std::hash<NES::VarianceAggregationLogicalFunction<NES::VarianceKind::Population>>;
template struct std::hash<NES::VarianceAggregationLogicalFunction<NES::VarianceKind::Sample>>;
