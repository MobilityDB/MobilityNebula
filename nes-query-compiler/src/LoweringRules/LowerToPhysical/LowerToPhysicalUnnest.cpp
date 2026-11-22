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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalUnnest.hpp>

#include <memory>
#include <vector>

#include <Functions/FunctionProvider.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/UnnestLogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <UnnestPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalUnnest::apply(LogicalOperator logicalOperator)
{
    const auto unnest = logicalOperator.getAs<UnnestLogicalOperator>();
    const auto array
        = QueryCompilation::FunctionProvider::lowerFunction(unnest->getArray(), *unnest->getChild()->getTraitSet().get<FieldMappingTrait>());
    const auto traitSet = logicalOperator.getTraitSet();

    /// The element field is the one field of the output its child does not have, named as the field mapping names it
    const auto elementField = unnest->getOutputSchema().getFieldByName(unnest->getElement());
    PRECONDITION(elementField.has_value(), "The output of UNNEST holds its element field");
    const auto elementName = traitSet.get<FieldMappingTrait>()->getMapping(unbind(elementField.value()));
    PRECONDITION(elementName.has_value(), "The element field of UNNEST was not in the field mapping");

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(unnest->getChild()->getTraitSet());

    auto physicalOperator = UnnestPhysicalOperator(array, elementName.value());
    const auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        inputSchema,
        outputSchema,
        memoryLayoutType,
        memoryLayoutType,
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leaves(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leaves = {leaves}};
}
}
