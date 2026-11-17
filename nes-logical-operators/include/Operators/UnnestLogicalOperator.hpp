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

/// CROSS JOIN UNNEST(array) AS alias(element): each record of the child, once per element of the array the function answers
/// for it, the element in the field `element`, as SQL:2003 writes it and PostgreSQL, DuckDB and Flink read it. The array is
/// a value of an array type a plugin adds over VARSIZED, whose ExtensionType::element is the type of its elements; an array
/// of no element gives no record. The output holds the child's fields and the element's.
class UnnestLogicalOperator : public ManagedByOperator
{
public:
    UnnestLogicalOperator(WeakLogicalOperator self, LogicalFunction array, Identifier alias, Identifier element);
    UnnestLogicalOperator(WeakLogicalOperator self, LogicalOperator child, LogicalFunction array, Identifier alias, Identifier element);

    static TypedLogicalOperator<UnnestLogicalOperator> create(LogicalFunction array, Identifier alias, Identifier element);
    static TypedLogicalOperator<UnnestLogicalOperator>
    create(LogicalOperator child, LogicalFunction array, Identifier alias, Identifier element);

    [[nodiscard]] LogicalFunction getArray() const;
    [[nodiscard]] Identifier getAlias() const;
    [[nodiscard]] Identifier getElement() const;

    [[nodiscard]] bool operator==(const UnnestLogicalOperator& rhs) const;

    [[nodiscard]] UnnestLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnnestLogicalOperator withChildrenUnsafe(std::vector<LogicalOperator> children) const;
    [[nodiscard]] UnnestLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;
    [[nodiscard]] LogicalOperator getChild() const;
    [[nodiscard]] Schema<Field, Unordered> getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UnnestLogicalOperator withInferredSchema() const;

private:
    static constexpr std::string_view NAME = "Unnest";
    std::optional<LogicalOperator> child;
    LogicalFunction array;
    Identifier alias;
    Identifier element;

    void inferLocalSchema();
    /// Set during schema inference
    std::optional<Schema<UnqualifiedUnboundField, Unordered>> outputSchema;

    TraitSet traitSet;
    friend struct std::hash<UnnestLogicalOperator>;
};

namespace detail
{
struct ReflectedUnnestLogicalOperator
{
    OperatorId operatorId{OperatorId::INVALID};
    LogicalFunction array;
    Identifier alias;
    Identifier element;
};
}

template <>
struct Reflector<TypedLogicalOperator<UnnestLogicalOperator>>
{
    Reflected operator()(const TypedLogicalOperator<UnnestLogicalOperator>& op, const ReflectionContext& context) const;
};

template <>
struct Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>
{
    using ContextType = std::shared_ptr<ReflectedPlan>;
    ContextType plan;
    explicit Unreflector(ContextType operatorMapping);
    TypedLogicalOperator<UnnestLogicalOperator> operator()(const Reflected& rfl, const ReflectionContext& context) const;
};

static_assert(LogicalOperatorConcept<UnnestLogicalOperator>);
}

template <>
struct std::hash<NES::UnnestLogicalOperator>
{
    uint64_t operator()(const NES::UnnestLogicalOperator& op) const noexcept;
};
