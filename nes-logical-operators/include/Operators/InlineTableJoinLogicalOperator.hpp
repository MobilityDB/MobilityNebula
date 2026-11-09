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

#include <optional>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Traits/Trait.hpp>
#include <Traits/TraitSet.hpp>
#include <Util/PlanRenderer.hpp>
#include <SerializableOperator.pb.h>

namespace NES
{

/// The rows of FROM t, (VALUES (a, b), (c, d)) AS f(x, y), or of t CROSS JOIN the same table: each row of its input once
/// per row of the table, the values of the row in the fields f$x and f$y. The values are functions, read row after row
/// and one per field, and every row gives each field a value of the same type.
class InlineTableJoinLogicalOperator
{
public:
    InlineTableJoinLogicalOperator(std::vector<LogicalFunction> values, std::vector<std::string> fieldNames);

    [[nodiscard]] const std::vector<LogicalFunction>& getValues() const;
    [[nodiscard]] const std::vector<std::string>& getFieldNames() const;

    [[nodiscard]] bool operator==(const InlineTableJoinLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] InlineTableJoinLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] InlineTableJoinLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] InlineTableJoinLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<FunctionList> INLINE_TABLE_VALUES{
            "inlineTableValues",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(INLINE_TABLE_VALUES, config); }};

        static inline const DescriptorConfig::ConfigParameter<FunctionList> INLINE_TABLE_FIELDS{
            "inlineTableFields",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(INLINE_TABLE_FIELDS, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(INLINE_TABLE_VALUES, INLINE_TABLE_FIELDS);
    };

private:
    static constexpr std::string_view NAME = "InlineTableJoin";
    std::vector<LogicalFunction> values;
    std::vector<std::string> fieldNames;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<InlineTableJoinLogicalOperator>);
}
