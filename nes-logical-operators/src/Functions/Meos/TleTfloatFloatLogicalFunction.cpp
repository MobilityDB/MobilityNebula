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

#include <Functions/Meos/TleTfloatFloatLogicalFunction.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <ErrorHandling.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <fmt/format.h>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

TleTfloatFloatLogicalFunction::TleTfloatFloatLogicalFunction(LogicalFunction value,
                                                                   LogicalFunction threshold,
                                                                   LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(3);
    parameters.push_back(std::move(value));
    parameters.push_back(std::move(threshold));
    parameters.push_back(std::move(ts));
}

DataType TleTfloatFloatLogicalFunction::getDataType() const { return dataType; }

LogicalFunction TleTfloatFloatLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> TleTfloatFloatLogicalFunction::getChildren() const { return parameters; }

LogicalFunction TleTfloatFloatLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "TleTfloatFloatLogicalFunction requires 3 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view TleTfloatFloatLogicalFunction::getType() const { return NAME; }

bool TleTfloatFloatLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TleTfloatFloatLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string TleTfloatFloatLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t index = 0; index < parameters.size(); ++index) {
        if (index > 0) args += ", ";
        args += parameters[index].explain(verbosity);
    }
    return fmt::format("{}({})", NAME, args);
}

LogicalFunction TleTfloatFloatLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
        newChildren.emplace_back(child.withInferredDataType(schema));
    return withChildren(newChildren);
}

SerializableFunction TleTfloatFloatLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTleTfloatFloatLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3,
                 "TleTfloatFloatLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    return TleTfloatFloatLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
