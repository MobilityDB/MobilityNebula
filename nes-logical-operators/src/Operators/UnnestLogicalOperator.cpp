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

#include <Operators/UnnestLogicalOperator.hpp>

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
#include <folly/hash/Hash.h>
#include <ErrorHandling.hpp>

namespace NES
{
UnnestLogicalOperator::UnnestLogicalOperator(WeakLogicalOperator self, LogicalFunction array, Identifier alias, Identifier element)
    : ManagedByOperator(std::move(self)), array(std::move(array)), alias(std::move(alias)), element(std::move(element))
{
}

UnnestLogicalOperator::UnnestLogicalOperator(
    WeakLogicalOperator self, LogicalOperator child, LogicalFunction array, Identifier alias, Identifier element)
    : ManagedByOperator(std::move(self))
    , child(std::move(child))
    , array(std::move(array))
    , alias(std::move(alias))
    , element(std::move(element))
{
    inferLocalSchema();
}

TypedLogicalOperator<UnnestLogicalOperator> UnnestLogicalOperator::create(LogicalFunction array, Identifier alias, Identifier element)
{
    return TypedLogicalOperator<UnnestLogicalOperator>{std::move(array), std::move(alias), std::move(element)};
}

TypedLogicalOperator<UnnestLogicalOperator>
UnnestLogicalOperator::create(LogicalOperator child, LogicalFunction array, Identifier alias, Identifier element)
{
    return TypedLogicalOperator<UnnestLogicalOperator>{std::move(child), std::move(array), std::move(alias), std::move(element)};
}

std::string_view UnnestLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction UnnestLogicalOperator::getArray() const
{
    return array;
}

Identifier UnnestLogicalOperator::getAlias() const
{
    return alias;
}

Identifier UnnestLogicalOperator::getElement() const
{
    return element;
}

bool UnnestLogicalOperator::operator==(const UnnestLogicalOperator& rhs) const
{
    return array == rhs.array && alias == rhs.alias && element == rhs.element && outputSchema == rhs.outputSchema
        && traitSet == rhs.traitSet;
};

std::string UnnestLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "UNNEST(opId: {}, array: {}, alias: {}, element: {}, traitSet: {})",
            opId,
            array.explain(verbosity),
            alias,
            element,
            traitSet.explain(verbosity));
    }
    return fmt::format("UNNEST({} AS {}({}))", array.explain(verbosity), alias, element);
}

void UnnestLogicalOperator::inferLocalSchema()
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    const auto inputSchema = child->getOutputSchema();
    array = array.withInferredDataType(inputSchema);
    const auto arrayType = array.getDataType();
    if (not arrayType.isExtension() or arrayType.extension->element == nullptr)
    {
        throw CannotInferSchema("UNNEST takes an array, but got a value of type {}", arrayType);
    }
    /// The element is a variable-sized value of the array's element type
    auto elementType = arrayType;
    elementType.extension = arrayType.extension->element;

    auto fields = unbind(inputSchema) | std::ranges::to<std::vector>();
    fields.emplace_back(element, elementType);
    auto outputSchemaOpt = Schema<UnqualifiedUnboundField, Unordered>::tryCreateCollisionFree(std::move(fields));
    if (!outputSchemaOpt.has_value())
    {
        throw CannotInferSchema(
            "The element of UNNEST collides with a field of its input: {}",
            Schema<UnqualifiedUnboundField, Unordered>::createCollisionString(outputSchemaOpt.error()));
    }
    outputSchema = std::move(outputSchemaOpt).value();
}

UnnestLogicalOperator UnnestLogicalOperator::withInferredSchema() const
{
    PRECONDITION(child.has_value(), "Child not set when calling schema inference");
    auto copy = *this;
    copy.child = copy.child->withInferredSchema();
    copy.inferLocalSchema();
    return copy;
}

TraitSet UnnestLogicalOperator::getTraitSet() const
{
    return traitSet;
}

UnnestLogicalOperator UnnestLogicalOperator::withTraitSet(TraitSet traitSet) const
{
    auto copy = *this;
    copy.traitSet = std::move(traitSet);
    return copy;
}

UnnestLogicalOperator UnnestLogicalOperator::withChildrenUnsafe(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for unnest, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    return copy;
}

UnnestLogicalOperator UnnestLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    PRECONDITION(children.size() == 1, "Can only set exactly one child for unnest, got {}", children.size());
    auto copy = *this;
    copy.child = std::move(children.at(0));
    copy.inferLocalSchema();
    return copy;
}

Schema<Field, Unordered> UnnestLogicalOperator::getOutputSchema() const
{
    INVARIANT(outputSchema.has_value(), "Accessed output schema before calling schema inference");
    return NES::bindToOperator(self.lock(), outputSchema.value());
}

std::vector<LogicalOperator> UnnestLogicalOperator::getChildren() const
{
    if (child.has_value())
    {
        return {*child};
    }
    return {};
}

LogicalOperator UnnestLogicalOperator::getChild() const
{
    PRECONDITION(child.has_value(), "Child not set when trying to retrieve child");
    return child.value();
}

Reflected Reflector<TypedLogicalOperator<UnnestLogicalOperator>>::operator()(
    const TypedLogicalOperator<UnnestLogicalOperator>& op, const ReflectionContext& context) const
{
    return context.reflect(detail::ReflectedUnnestLogicalOperator{
        .operatorId = op.getId(), .array = op->getArray(), .alias = op->getAlias(), .element = op->getElement()});
}

Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>::Unreflector(ContextType operatorMapping) : plan(std::move(operatorMapping))
{
}

TypedLogicalOperator<UnnestLogicalOperator>
Unreflector<TypedLogicalOperator<UnnestLogicalOperator>>::operator()(const Reflected& rfl, const ReflectionContext& context) const
{
    auto [id, array, alias, element] = context.unreflect<detail::ReflectedUnnestLogicalOperator>(rfl);
    auto children = plan->getChildrenFor(id, context);
    if (children.size() != 1)
    {
        throw CannotDeserialize("UnnestLogicalOperator requires exactly one child, but got {}", children.size());
    }
    return UnnestLogicalOperator::create(children.at(0), array, alias, element);
}
}

uint64_t std::hash<NES::UnnestLogicalOperator>::operator()(const NES::UnnestLogicalOperator& op) const noexcept
{
    return folly::hash::hash_combine(op.getArray(), op.getAlias(), op.getElement());
}
