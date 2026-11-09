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

#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Operators/LogicalOperator.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>
#include <Serialization/SchemaSerializationUtil.hpp>
#include <Traits/Trait.hpp>
#include <Util/PlanRenderer.hpp>
#include <ErrorHandling.hpp>
#include <LogicalOperatorRegistry.hpp>
#include <SerializableOperator.pb.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

UnnestLogicalOperator::UnnestLogicalOperator(LogicalFunction array, std::string elementFieldName)
    : array(std::move(array)), elementFieldName(std::move(elementFieldName))
{
}

std::string_view UnnestLogicalOperator::getName() const noexcept
{
    return NAME;
}

LogicalFunction UnnestLogicalOperator::getArray() const
{
    return array;
}

const std::string& UnnestLogicalOperator::getElementFieldName() const
{
    return elementFieldName;
}

bool UnnestLogicalOperator::operator==(const UnnestLogicalOperator& rhs) const
{
    return array == rhs.array && elementFieldName == rhs.elementFieldName && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
};

std::string UnnestLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "UNNEST(opId: {}, array: {}, element: {}, traitSet: {})",
            opId,
            array.explain(verbosity),
            elementFieldName,
            traitSet.explain(verbosity));
    }
    return fmt::format("UNNEST({} AS {})", array.explain(verbosity), elementFieldName);
}

UnnestLogicalOperator UnnestLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("Unnest should have at least one input");
    }

    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for Unnest operator");
        }
    }

    copy.array = array.withInferredDataType(firstSchema);
    const auto arrayType = copy.array.getDataType();
    if (arrayType.extension == nullptr or arrayType.extension->element == nullptr)
    {
        throw CannotInferSchema("UNNEST takes an array, not {}", arrayType);
    }
    auto elementType = arrayType;
    elementType.extension = arrayType.extension->element;
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    copy.outputSchema = copy.outputSchema.addField(elementFieldName, elementType);
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

UnnestLogicalOperator UnnestLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> UnnestLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema UnnestLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> UnnestLogicalOperator::getChildren() const
{
    return children;
}

void UnnestLogicalOperator::serialize(SerializableOperator& serializableOperator) const
{
    SerializableLogicalOperator proto;

    proto.set_operator_type(NAME);

    for (const auto& input : getInputSchemas())
    {
        auto* schProto = proto.add_input_schemas();
        SchemaSerializationUtil::serializeSchema(input, schProto);
    }

    auto* outSch = proto.mutable_output_schema();
    SchemaSerializationUtil::serializeSchema(outputSchema, outSch);

    for (auto& child : getChildren())
    {
        serializableOperator.add_children_ids(child.getId().getRawValue());
    }

    FunctionList funcList;
    *funcList.add_functions() = array.serialize();
    const DescriptorConfig::ConfigType funcVariant = std::move(funcList);
    (*serializableOperator.mutable_config())[ConfigParameters::UNNEST_ARRAY_FUNCTION] = descriptorConfigTypeToProto(funcVariant);

    const DescriptorConfig::ConfigType fieldVariant = elementFieldName;
    (*serializableOperator.mutable_config())[ConfigParameters::UNNEST_ELEMENT_FIELD] = descriptorConfigTypeToProto(fieldVariant);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType LogicalOperatorGeneratedRegistrar::RegisterUnnestLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto functionVariant = arguments.config.at(UnnestLogicalOperator::ConfigParameters::UNNEST_ARRAY_FUNCTION);
    auto fieldVariant = arguments.config.at(UnnestLogicalOperator::ConfigParameters::UNNEST_ELEMENT_FIELD);
    if (std::holds_alternative<FunctionList>(functionVariant) and std::holds_alternative<std::string>(fieldVariant))
    {
        const auto functions = std::get<FunctionList>(functionVariant).functions();
        if (functions.size() != 1)
        {
            throw CannotDeserialize("Expected exactly one function but got {}", functions.size());
        }
        auto function = FunctionSerializationUtil::deserializeFunction(functions[0]);
        auto logicalOperator = UnnestLogicalOperator(function, std::get<std::string>(fieldVariant));
        return logicalOperator.withInferredSchema(arguments.inputSchemas);
    }
    throw UnknownLogicalOperator();
}
}
