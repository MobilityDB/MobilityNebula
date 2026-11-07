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

#include <Functions/Meos/MeosLogicalFunction.hpp>

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
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

MeosLogicalFunction::MeosLogicalFunction(
    const std::string_view name, const std::span<const MeosOverload> overloads, std::vector<LogicalFunction> children)
    : name(name), overloads(overloads), children(std::move(children))
{
    /// A deserialized function's children carry their inferred types already
    chosen = chooseOverload();
    if (chosen)
    {
        dataType = DataTypeProvider::provideDataType(std::string(overloads[*chosen].result));
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
            const bool same = childType == argumentType;
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

bool MeosLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const MeosLogicalFunction*>(&rhs))
    {
        return name == other->name and chosen == other->chosen and children == other->children;
    }
    return false;
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

LogicalFunction MeosLogicalFunction::withDataType(const DataType& dataType) const
{
    auto copy = *this;
    copy.dataType = dataType;
    return copy;
}

LogicalFunction MeosLogicalFunction::withInferredDataType(const Schema& schema) const
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
    copy.dataType = DataTypeProvider::provideDataType(std::string(overloads[*copy.chosen].result));
    return copy;
}

std::vector<LogicalFunction> MeosLogicalFunction::getChildren() const
{
    return children;
}

LogicalFunction MeosLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    auto copy = *this;
    copy.children = children;
    copy.chosen = copy.chooseOverload();
    return copy;
}

std::string_view MeosLogicalFunction::getType() const
{
    return chosen ? overloads[*chosen].physicalFunction : name;
}

SerializableFunction MeosLogicalFunction::serialize() const
{
    SerializableFunction serializedFunction;
    serializedFunction.set_function_type(std::string(name));
    for (const auto& child : children)
    {
        serializedFunction.add_children()->CopyFrom(child.serialize());
    }
    DataTypeSerializationUtil::serializeDataType(getDataType(), serializedFunction.mutable_data_type());
    return serializedFunction;
}

}
