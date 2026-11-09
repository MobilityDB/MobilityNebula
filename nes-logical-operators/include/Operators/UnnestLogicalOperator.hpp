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

/// The rows of FROM t CROSS JOIN UNNEST(array) AS u(x): each row of its input once per element of the array the function
/// answers for it, with the element in the field x of u. The array is of a type a plugin adds over VARSIZED that states
/// the type of its elements (ExtensionType::element), and an array of no element gives no row.
class UnnestLogicalOperator
{
public:
    UnnestLogicalOperator(LogicalFunction array, std::string elementFieldName);

    [[nodiscard]] LogicalFunction getArray() const;
    [[nodiscard]] const std::string& getElementFieldName() const;

    [[nodiscard]] bool operator==(const UnnestLogicalOperator& rhs) const;
    void serialize(SerializableOperator&) const;

    [[nodiscard]] UnnestLogicalOperator withTraitSet(TraitSet traitSet) const;
    [[nodiscard]] TraitSet getTraitSet() const;

    [[nodiscard]] UnnestLogicalOperator withChildren(std::vector<LogicalOperator> children) const;
    [[nodiscard]] std::vector<LogicalOperator> getChildren() const;

    [[nodiscard]] std::vector<Schema> getInputSchemas() const;
    [[nodiscard]] Schema getOutputSchema() const;

    [[nodiscard]] std::string explain(ExplainVerbosity verbosity, OperatorId) const;
    [[nodiscard]] std::string_view getName() const noexcept;

    [[nodiscard]] UnnestLogicalOperator withInferredSchema(std::vector<Schema> inputSchemas) const;

    struct ConfigParameters
    {
        static inline const DescriptorConfig::ConfigParameter<std::string> UNNEST_ARRAY_FUNCTION{
            "unnestArrayFunction",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(UNNEST_ARRAY_FUNCTION, config); }};

        static inline const DescriptorConfig::ConfigParameter<std::string> UNNEST_ELEMENT_FIELD{
            "unnestElementField",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return DescriptorConfig::tryGet(UNNEST_ELEMENT_FIELD, config); }};

        static inline std::unordered_map<std::string, DescriptorConfig::ConfigParameterContainer> parameterMap
            = DescriptorConfig::createConfigParameterContainerMap(UNNEST_ARRAY_FUNCTION, UNNEST_ELEMENT_FIELD);
    };

private:
    static constexpr std::string_view NAME = "Unnest";
    LogicalFunction array;
    std::string elementFieldName;

    std::vector<LogicalOperator> children;
    TraitSet traitSet;
    Schema inputSchema, outputSchema;
};

static_assert(LogicalOperatorConcept<UnnestLogicalOperator>);
}
