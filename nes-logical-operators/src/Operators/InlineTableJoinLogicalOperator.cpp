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
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <Configurations/Descriptor.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
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

InlineTableJoinLogicalOperator::InlineTableJoinLogicalOperator(std::vector<LogicalFunction> values, std::vector<std::string> fieldNames)
    : values(std::move(values)), fieldNames(std::move(fieldNames))
{
}

std::string_view InlineTableJoinLogicalOperator::getName() const noexcept
{
    return NAME;
}

const std::vector<LogicalFunction>& InlineTableJoinLogicalOperator::getValues() const
{
    return values;
}

const std::vector<std::string>& InlineTableJoinLogicalOperator::getFieldNames() const
{
    return fieldNames;
}

bool InlineTableJoinLogicalOperator::operator==(const InlineTableJoinLogicalOperator& rhs) const
{
    return values == rhs.values && fieldNames == rhs.fieldNames && getOutputSchema() == rhs.getOutputSchema()
        && getInputSchemas() == rhs.getInputSchemas() && getTraitSet() == rhs.getTraitSet();
};

std::string InlineTableJoinLogicalOperator::explain(ExplainVerbosity verbosity, OperatorId opId) const
{
    const auto rows = fieldNames.empty() ? 0 : values.size() / fieldNames.size();
    if (verbosity == ExplainVerbosity::Debug)
    {
        return fmt::format(
            "INLINE_TABLE_JOIN(opId: {}, fields: {}, rows: {}, traitSet: {})",
            opId,
            fmt::join(fieldNames, ", "),
            rows,
            traitSet.explain(verbosity));
    }
    return fmt::format("INLINE_TABLE_JOIN({} rows AS {})", rows, fmt::join(fieldNames, ", "));
}

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withInferredSchema(std::vector<Schema> inputSchemas) const
{
    auto copy = *this;
    if (inputSchemas.empty())
    {
        throw CannotDeserialize("InlineTableJoin should have at least one input");
    }

    const auto& firstSchema = inputSchemas.at(0);
    for (const auto& schema : inputSchemas)
    {
        if (schema != firstSchema)
        {
            throw CannotInferSchema("All input schemas must be equal for InlineTableJoin operator");
        }
    }
    if (fieldNames.empty() or values.empty() or values.size() % fieldNames.size() != 0)
    {
        throw CannotInferSchema("An inline table of {} fields cannot hold {} values", fieldNames.size(), values.size());
    }

    copy.values.clear();
    for (const auto& value : values)
    {
        copy.values.push_back(value.withInferredDataType(firstSchema));
    }
    copy.inputSchema = firstSchema;
    copy.outputSchema = firstSchema;
    /// Each field takes the type of its value in the first row, and every other row gives it a value of that type
    for (size_t field = 0; field < fieldNames.size(); ++field)
    {
        const auto fieldType = copy.values[field].getDataType();
        for (size_t at = field + fieldNames.size(); at < copy.values.size(); at += fieldNames.size())
        {
            if (copy.values[at].getDataType() != fieldType)
            {
                throw CannotInferSchema(
                    "The field {} of an inline table holds a {} and a {}", fieldNames[field], fieldType, copy.values[at].getDataType());
            }
        }
        copy.outputSchema = copy.outputSchema.addField(fieldNames[field], fieldType);
    }
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

InlineTableJoinLogicalOperator InlineTableJoinLogicalOperator::withChildren(std::vector<LogicalOperator> children) const
{
    auto copy = *this;
    copy.children = std::move(children);
    return copy;
}

std::vector<Schema> InlineTableJoinLogicalOperator::getInputSchemas() const
{
    return {inputSchema};
};

Schema InlineTableJoinLogicalOperator::getOutputSchema() const
{
    return outputSchema;
}

std::vector<LogicalOperator> InlineTableJoinLogicalOperator::getChildren() const
{
    return children;
}

void InlineTableJoinLogicalOperator::serialize(SerializableOperator& serializableOperator) const
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

    FunctionList valueList;
    for (const auto& value : values)
    {
        *valueList.add_functions() = value.serialize();
    }
    const DescriptorConfig::ConfigType valuesVariant = std::move(valueList);
    (*serializableOperator.mutable_config())[ConfigParameters::INLINE_TABLE_VALUES] = descriptorConfigTypeToProto(valuesVariant);

    /// The fields are named as the keys of a window aggregation are, by field accesses
    FunctionList fieldList;
    for (const auto& fieldName : fieldNames)
    {
        *fieldList.add_functions() = LogicalFunction(FieldAccessLogicalFunction(fieldName)).serialize();
    }
    const DescriptorConfig::ConfigType fieldsVariant = std::move(fieldList);
    (*serializableOperator.mutable_config())[ConfigParameters::INLINE_TABLE_FIELDS] = descriptorConfigTypeToProto(fieldsVariant);

    serializableOperator.mutable_operator_()->CopyFrom(proto);
}

LogicalOperatorRegistryReturnType
LogicalOperatorGeneratedRegistrar::RegisterInlineTableJoinLogicalOperator(LogicalOperatorRegistryArguments arguments)
{
    auto valuesVariant = arguments.config.at(InlineTableJoinLogicalOperator::ConfigParameters::INLINE_TABLE_VALUES);
    auto fieldsVariant = arguments.config.at(InlineTableJoinLogicalOperator::ConfigParameters::INLINE_TABLE_FIELDS);
    if (not std::holds_alternative<FunctionList>(valuesVariant) or not std::holds_alternative<FunctionList>(fieldsVariant))
    {
        throw UnknownLogicalOperator();
    }
    std::vector<LogicalFunction> values;
    for (const auto& value : std::get<FunctionList>(valuesVariant).functions())
    {
        values.push_back(FunctionSerializationUtil::deserializeFunction(value));
    }
    std::vector<std::string> fieldNames;
    for (const auto& field : std::get<FunctionList>(fieldsVariant).functions())
    {
        auto fieldAccess = FunctionSerializationUtil::deserializeFunction(field).tryGet<FieldAccessLogicalFunction>();
        if (not fieldAccess)
        {
            throw UnknownLogicalOperator();
        }
        fieldNames.push_back(fieldAccess->getFieldName());
    }
    auto logicalOperator = InlineTableJoinLogicalOperator(std::move(values), std::move(fieldNames));
    return logicalOperator.withInferredSchema(arguments.inputSchemas);
}
}
