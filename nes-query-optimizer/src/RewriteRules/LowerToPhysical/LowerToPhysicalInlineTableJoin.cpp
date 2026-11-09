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

#include <RewriteRules/LowerToPhysical/LowerToPhysicalInlineTableJoin.hpp>

#include <memory>
#include <vector>
#include <Functions/FunctionProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Operators/InlineTableJoinLogicalOperator.hpp>
#include <Operators/LogicalOperator.hpp>
#include <RewriteRules/AbstractRewriteRule.hpp>
#include <ErrorHandling.hpp>
#include <InlineTableJoinPhysicalOperator.hpp>
#include <PhysicalOperator.hpp>
#include <RewriteRuleRegistry.hpp>

namespace NES
{

RewriteRuleResultSubgraph LowerToPhysicalInlineTableJoin::apply(LogicalOperator logicalOperator)
{
    PRECONDITION(logicalOperator.tryGetAs<InlineTableJoinLogicalOperator>(), "Expected an InlineTableJoinLogicalOperator");
    auto join = logicalOperator.getAs<InlineTableJoinLogicalOperator>();
    std::vector<PhysicalFunction> values;
    for (const auto& value : join->getValues())
    {
        values.push_back(QueryCompilation::FunctionProvider::lowerFunction(value));
    }
    auto physicalOperator = InlineTableJoinPhysicalOperator(std::move(values), join->getFieldNames());
    auto wrapper = std::make_shared<PhysicalOperatorWrapper>(
        physicalOperator,
        logicalOperator.getInputSchemas()[0],
        logicalOperator.getOutputSchema(),
        PhysicalOperatorWrapper::PipelineLocation::INTERMEDIATE);

    /// Creates a physical leaf for each logical leaf. Required, as this operator can have any number of sources.
    std::vector leafes(logicalOperator.getChildren().size(), wrapper);
    return {.root = wrapper, .leafs = {leafes}};
};

std::unique_ptr<AbstractRewriteRule>
RewriteRuleGeneratedRegistrar::RegisterInlineTableJoinRewriteRule(RewriteRuleRegistryArguments argument) /// NOLINT
{
    return std::make_unique<LowerToPhysicalInlineTableJoin>(argument.conf);
}
}
