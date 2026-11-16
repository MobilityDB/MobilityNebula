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

#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/UnboundField.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifier.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/LogicalOperatorFwd.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Serialization/ReflectedOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>

namespace NES
{

/// FROM t, (VALUES (a, b), (c, d)) AS alias(x, y), or CROSS JOIN the same table: each record of the child, once per row of
/// the table, the values of the row in the fields x and y, as SQL writes the cross join with a table of values. A value is
/// an expression over the fields of the child; each field takes the type of its value in the first row, and every other
/// row gives it a value of that type. The output holds the child's fields and the table's.
class InlineTableJoinLogicalOperator : public ManagedByOperator
{
public:
    InlineTableJoinLogicalOperator(WeakLogicalOperator self, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields);
    InlineTableJoinLogicalOperator(
        WeakLogicalOperator self, LogicalOperator child, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields);

    static TypedLogicalOperator<InlineTableJoinLogicalOperator>
    create(std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields);
    static TypedLogicalOperator<InlineTableJoinLogicalOperator>
    create(LogicalOperator child, std::vector<LogicalFunction> values, Identifier alias, std::vector<Identifier> fields);

    /// The values of the table, row after row
    [[nodiscard]] const std::vector<LogicalFunction>& getValues() const;
    [[nodiscard]] Identifier getAlias() const;
    [[nodiscard]] const std::vector<Identifier>& getFields() const;

    [[nodiscard]] bool operator==(const InlineTableJoinLogicalOperator& rhs) const;

    [[nodiscard]] InlineTableJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineTableJoinLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] InlineTableJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] InlineTableJoinLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "InlineTableJoin";
    std::optional<LogicalOperator> child;
    std::vector<LogicalFunction> values;
    Identifier alias;
    std::vector<Identifier> fields;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<InlineTableJoinLogicalOperator>;
};

namespace detail
{
struct ReflectedInlineTableJoinLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    std::vector<LogicalFunction> values;
    Identifier alias;
    std::vector<Identifier> fields;
};
}

template <>
struct Reflector<TypedLogicalOperator<InlineTableJoinLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<InlineTableJoinLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<InlineTableJoinLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<InlineTableJoinLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<InlineTableJoinLogicalOperator>);
}

template <>
struct std::hash<NES::InlineTableJoinLogicalOperator>
{
    uint64_t operator()(const NES::InlineTableJoinLogicalOperator& op) const noexcept;
};
