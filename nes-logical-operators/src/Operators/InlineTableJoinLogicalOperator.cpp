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

#include <Operators/InlineTableJoinLogicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <optional>
#include <ranges>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Binder.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/LogicalFunctionReflection.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{
InlineTableJoinLogicalOperator::InlineTableJoinLogicalOperator(
    WeakLogicalOperator self, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields)
    : ManagedByOperator(std::move(self)), values(std::move(values)), alias(std::move(alias)), fields(std::move(fields))
{
}

InlineTableJoinLogicalOperator::InlineTableJoinLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , values(std::move(values))
    , alias(std::move(alias))
    , fields(std::move(fields))
{
    inferLocalSchema();
}

TypedLogicalOperator<InlineTableJoinLogicalOperator>
InlineTableJoinLogicalOperator::create(std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields)
{
    return TypedLogicalOperator<InlineTableJoinLogicalOperator>{std::move(values), std::move(alias), std::move(fields)};
}

TypedLogicalOperator<InlineTableJoinLogicalOperator>
InlineTableJoinLogicalOperator::create(LogicalOperator child, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields)
{
    return TypedLogicalOperator<InlineTableJoinLogicalOperator>{std::move(child), std::move(values), std::move(alias), std::move(fields)};
}

std::string_view InlineTableJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::vector<LogicalFunction>& InlineTableJoinLogicalOperator::getValues() const
{
    return values;
}

Identifier InlineTableJoinLogicalOperator::getAlias() const
{
    return alias;
}

const std::vector<Identifier>& InlineTableJoinLogicalOperator::getFields() const
{
    return fields;
}

bool InlineTableJoinLogicalOperator::operator==(const InlineTableJoinLogicalOperator& rhs) const
{
    return values == rhs.values && alias == rhs.alias && fields == rhs.fields && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet;
};

std::string InlineTableJoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto rows = fields.empty() ? 0 : values.size() / fields.size();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INLINE_TABLE_JOIN(opId: {}, rows: {}, alias: {}, fields: {}, traitSet: {})",
            opId,
            rows,
            alias,
            fmt::join(fields, ", "),
            traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_TABLE_JOIN({} rows AS {}({}))", rows, alias, fmt::join(fields, ", "));
}

void InlineTableJoinLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    if (fields.empty() or values.empty() or values.size() % fields.size() != 0)
    {
        throw CannotInferSchema("An inline table of {} fields cannot hold {} values", fields.size(), values.size());
    }
    for (auto& value : values)
    {
        value = value.withInferredDataType(inputSchema);
    }
    auto outputFields = unbind(inputSchema) | std::ranges::to<std::vector>();
    /// Each field takes the type of its value in the first row, and every other row gives it a value of that type
    for (size_t field = 0; field < fields.size(); ++field)
    {
        const auto fieldType = values[field].getDataType();
        for (size_t at = field + fields.size(); at < values.size(); at += fields.size())
        {
            if (values[at].getDataType() != fieldType)
            {
                throw CannotInferSchema(
                    "The field {} of {} takes a value of type {} in its first row but {} in another",
                    fields[field],
                    alias,
                    fieldType,
                    values[at].getDataType());
            }
        }
        outputFields.emplace_back(fields[field], fieldType);
    }
    auto outputSchemaOpt = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(std::move(outputFields));
    if (!outputSchemaOpt.has_value())
    {
        throw CannotInferSchema(
            "A field of the inline table collides with a field of its input: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOpt.error()));
    }
    outputSchema = std::move(outputSchemaOpt).value();
}

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet InlineTableJoinLogicalOperator::getTraitSet() const
{
    return traitSet;
}

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for an inline table join, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for an inline table join, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> InlineTableJoinLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> InlineTableJoinLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator InlineTableJoinLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<InlineTableJoinLogicalOperator>>::operator()(
    const TypedLogicalOperator<InlineTableJoinLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedInlineTableJoinLogicalOperator{
        .operatorId = op.getId(), .values = op->getValues(), .alias = op->getAlias(), .fields = op->getFields()});
}

Unreflector<TypedLogicalOperator<InlineTableJoinLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<InlineTableJoinLogicalOperator>
Unreflector<TypedLogicalOperator<InlineTableJoinLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, values, alias, fields] = context.unreflect<detail::ReflectedInlineTableJoinLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("InlineTableJoinLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return InlineTableJoinLogicalOperator::create(children.at(0), values, alias, fields);
}
}

uint64_t std::hash<NES::InlineTableJoinLogicalOperator>::operator()(const NES::InlineTableJoinLogicalOperator& op) const noexcept
{
    auto hash = std::hash<NES::Identifier>{}(op.getAlias());
    for (const auto& value : op.getValues())
    {
        hash = folly::hash::hash_combine(hash, value);
    }
    for (const auto& field : op.getFields())
    {
        hash = folly::hash::hash_combine(hash, field);
    }
    return hash;
}
