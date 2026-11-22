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

#include <Functions/MeosLogicalFunction.hpp>

#include <algorithm>
#include <cstddef>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
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
}

std::optional<MeosFunction> meosFunction(const std::string_view name)
{
    const auto functions = meosFunctions();
    const auto found = std::ranges::lower_bound(functions, name, {}, &MeosFunction::name);
    if (found == functions.end() or found->name != name)
    {
        return std::nullopt;
    }
    return *found;
}

MeosLogicalFunction::MeosLogicalFunction(
    const std::string_view name, const std::span<const MeosOverload> overloads, std::vector<LogicalFunction> children)
    : name(name), overloads(overloads), children(std::move(children))
{
    /// An unreflected function's children carry their inferred types already
    chosen = chooseOverload();
    if (chosen)
    {
        dataType = answerType();
    }
}

std::optional<size_t> MeosLogicalFunction::chooseOverload() const
{
    if (std::ranges::any_of(children, [](const auto& child) { return child.getDataType().isType(DataType::Type::UNDEFINED); }))
    {
        return std::nullopt;
    }
    const auto argumentsMatch = [this](const MeosOverload& overload, const bool castNumbers)
    {
        if (overload.arguments.size() != children.size())
        {
            return false;
        }
        for (size_t i = 0; i < children.size(); ++i)
        {
            const auto childType = children[i].getDataType();
            const auto argumentType = DataTypeProvider::provideDataType(std::string(overload.arguments[i]));
            const bool same = sameType(childType, argumentType);
            const bool castable = castNumbers and childType.isNumeric() and argumentType.isNumeric();
            if (not same and not castable)
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
            if (argumentsMatch(overloads[i], castNumbers))
            {
                return i;
            }
        }
    }
    return std::nullopt;
}

DataType MeosLogicalFunction::answerType() const
{
    const bool nullable = std::ranges::any_of(children, [](const auto& child) { return child.getDataType().nullable; });
    return DataTypeProvider::provideDataType(
        std::string(overloads[*chosen].result), nullable ? DataType::NULLABLE::IS_NULLABLE : DataType::NULLABLE::NOT_NULLABLE);
}

bool MeosLogicalFunction::operator==(const MeosLogicalFunction& rhs) const
{
    return name == rhs.name and chosen == rhs.chosen and children == rhs.children;
}

std::string MeosLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format(
        "{}({})",
        name,
        fmt::join(children | std::views::transform([verbosity](const auto& child) { return child.explain(verbosity); }), ", "));
}

DataType MeosLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction MeosLogicalFunction::withInferredDataType(const Schema<Field, Unordered>& schema) const
{
    auto copy = *this;
    copy.children.clear();
    for (const auto& child : children)
    {
        copy.children.push_back(child.withInferredDataType(schema));
    }
    copy.chosen = copy.chooseOverload();
    if (not copy.chosen)
    {
        throw FunctionNotImplemented(
            "{} takes no arguments of types ({})",
            name,
            fmt::join(copy.children | std::views::transform([](const auto& child) { return fmt::format("{}", child.getDataType()); }), ", "));
    }
    copy.dataType = copy.answerType();
    return copy;
}

std::vector<LogicalFunction> MeosLogicalFunction::getChildren() const
{
    return children;
}

MeosLogicalFunction MeosLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    copy.chosen = copy.chooseOverload();
    if (copy.chosen)
    {
        copy.dataType = copy.answerType();
    }
    return copy;
}

std::string_view MeosLogicalFunction::getType() const
{
    return chosen ? overloads[*chosen].physicalFunction : name;
}

Reflected Reflector<MeosLogicalFunction>::operator()(const MeosLogicalFunction& function, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedMeosLogicalFunction{.name = std::string(function.getName()), .children = function.getChildren()});
}

MeosLogicalFunction Unreflector<MeosLogicalFunction>::operator()(const Reflected& reflected, const ReflectionContext& context) const
{
    auto [name, children] = context.unreflect<detail::ReflectedMeosLogicalFunction>(reflected);
    /// The name the function keeps is the catalog's, of static storage as its signatures are
    const auto function = meosFunction(name);
    if (not function)
    {
        throw CannotDeserialize("{} is not a MEOS function", name);
    }
    return MeosLogicalFunction{function->name, function->overloads, std::move(children)};
}

}
