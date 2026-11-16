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

#include <LoweringRules/LowerToPhysical/LowerToPhysicalInlineTableJoin.hpp>

#include <memory>
#include <utility>
#include <vector>

#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <LoweringRules/AbstractLoweringRule.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Operators/InlineTableJoinLogicalOperator.hpp>
#include <Schema/Binder.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Traits/MemoryLayoutTypeTrait.hpp>
#include <Util/SchemaFactory.hpp>
#include <ErrorHandling.hpp>
#include <LoweringRuleRegistry.hpp>
#include <PhysicalOperator.hpp>
#include <InlineTableJoinPhysicalOperator.hpp>

namespace NES
{

LoweringRuleResultSubgraph LowerToPhysicalInlineTableJoin::apply(LogicalOperator logicalOperator)
{
    const auto join = logicalOperator.getAs<InlineTableJoinLogicalOperator>();
    const auto& childMapping = *join->getChild()->getTraitSet().get<FieldMappingTrait>();
    std::vector<PhysicalFunction> values;
    for (const auto& value : join->getValues())
    {
        values.push_back(QueryCompilation::FunctionProvider::lowerFunction(value, childMapping));
    }
    const auto traitSet = logicalOperator.getTraitSet();

    /// The fields of the table are the fields of the output its child does not have, named as the field mapping names them
    std::vector<Record::RecordFieldIdentifier> fieldNames;
    for (const auto& fieldName : join->getFields())
    {
        const auto field = join->getOutputSchema().getFieldByName(fieldName);
        PRECONDITION(field.has_value(), "The output of an inline table join holds the fields of its table");
        const auto mapped = traitSet.get<FieldMappingTrait>()->getMapping(unbind(field.value()));
        PRECONDITION(mapped.has_value(), "A field of an inline table join was not in the field mapping");
        fieldNames.push_back(mapped.value());
    }

    const auto memoryLayoutType = traitSet.get<MemoryLayoutTypeTrait>()->memoryLayout;
    const auto outputSchema = createPhysicalOutputSchema(traitSet);
    const auto inputSchema = createPhysicalOutputSchema(join->getChild()->getTraitSet());

    auto physicalOperator = InlineTableJoinPhysicalOperator(std::move(values), std::move(fieldNames));
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
