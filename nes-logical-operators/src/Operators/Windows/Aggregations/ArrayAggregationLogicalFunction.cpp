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

#include <Operators/Windows/Aggregations/ArrayAggregationLogicalFunction.hpp>

#include <cstddef>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

#include <utility>
#include <variant>
#include <DataTypes/DataType.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <folly/hash/Hash.h>
#include <AggregationLogicalFunctionRegistry.hpp>

namespace NES
{
namespace
{
/// The array type whose elements are values of the type, as a plugin declares one (ExtensionType::element); none for a
/// type of no array
std::optional<DataType> arrayTypeOf(const DataType& type)
{
    if (not type.isExtension())
    {
        return std::nullopt;
    }
    for (const auto& name : DataTypeRegistry::instance().getRegisteredNames())
    {
        const auto candidate = DataTypeProvider::tryProvideDataType(name, DataType::NULLABLE::NOT_NULLABLE);
        if (candidate and candidate->isExtension() and candidate->extension->element == type.extension)
        {
            return candidate;
        }
    }
    return std::nullopt;
}
}

ArrayAggregationLogicalFunction::ArrayAggregationLogicalFunction(AggregationFieldAccess inputFunction)
    : inputFunction(std::move(inputFunction))
{
}

std::string_view ArrayAggregationLogicalFunction::getName() noexcept
{
    return NAME;
}

DataType ArrayAggregationLogicalFunction::getAggregateType() const
{
    /// The array of the type of the input once it is bound; a window of no non-null value answers the empty array
    if (const auto* bound = std::get_if<TypedLogicalFunction<FieldAccessLogicalFunction>>(&inputFunction))
    {
        if (const auto array = arrayTypeOf((*bound)->getDataType()))
        {
            return *array;
        }
    }
    return DataTypeProvider::provideDataType(finalAggregateStampType, DataType::NULLABLE::NOT_NULLABLE);
}

bool ArrayAggregationLogicalFunction::shallIncludeNullValues() noexcept
{
    return false;
}

AggregationFieldAccess ArrayAggregationLogicalFunction::getInputFunction() const
{
    return inputFunction;
}

std::string ArrayAggregationLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    if (verbosity == ExplainVerbosity::Short)
    {
        return fmt::format("{}()", NAME);
    }
    auto inputExplain = std::visit([verbosity](const auto& input) { return input->explain(verbosity); }, inputFunction);
    return fmt::format("{}({})", NAME, inputExplain);
}

bool ArrayAggregationLogicalFunction::operator==(const ArrayAggregationLogicalFunction& other) const
{
    return inputFunction == other.inputFunction;
}

ArrayAggregationLogicalFunction ArrayAggregationLogicalFunction::withInferredType(const Schema<Field, Unordered>& schema) const
{
    auto newInputFunction = inferFieldAccess(inputFunction, schema);
    if (not arrayTypeOf(newInputFunction->getDataType()))
    {
        const auto& type = newInputFunction->getDataType();
        throw UnsupportedQuery(
            "{} is only supported on values of a type that has an array type, but field {} has type {}",
            NAME,
            newInputFunction->getField().getLastName(),
            type.isExtension() ? std::string(type.extension->name) : std::string(magic_enum::enum_name(type.type)));
    }
    return ArrayAggregationLogicalFunction{newInputFunction};
}

Reflected Reflector<ArrayAggregationLogicalFunction>::operator()(
    const ArrayAggregationLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(function.getInputFunction());
}

ArrayAggregationLogicalFunction
Unreflector<ArrayAggregationLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    return ArrayAggregationLogicalFunction{context.unreflect<AggregationFieldAccess>(reflected)};
}

AggregationLogicalFunctionRegistryReturnType ArrayAggregationLogicalFunction::create(AggregationLogicalFunctionRegistryArguments arguments)
{
    if (arguments.on.size() != 1)
    {
        throw CannotDeserialize("ArrayAggregationLogicalFunction requires exactly one field, but got {}", arguments.on.size());
    }
    return ArrayAggregationLogicalFunction{arguments.on.at(0)};
}
}

size_t
std::hash<NES::ArrayAggregationLogicalFunction>::operator()(const NES::ArrayAggregationLogicalFunction& aggregationFunction) const noexcept
{
    return folly::hash::hash_combine(aggregationFunction.getInputFunction(), NES::ArrayAggregationLogicalFunction::getName());
}
