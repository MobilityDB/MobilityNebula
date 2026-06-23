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

#include <Functions/Meos/EverNeTbigintTbigintLogicalFunction.hpp>

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

EverNeTbigintTbigintLogicalFunction::EverNeTbigintTbigintLogicalFunction(LogicalFunction value1,
                                                                     LogicalFunction value2,
                                                                     LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(3);
    parameters.push_back(std::move(value1));
    parameters.push_back(std::move(value2));
    parameters.push_back(std::move(ts));
}

DataType EverNeTbigintTbigintLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EverNeTbigintTbigintLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EverNeTbigintTbigintLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EverNeTbigintTbigintLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3, "EverNeTbigintTbigintLogicalFunction requires 3 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EverNeTbigintTbigintLogicalFunction::getType() const { return NAME; }

bool EverNeTbigintTbigintLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EverNeTbigintTbigintLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EverNeTbigintTbigintLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t index = 0; index < parameters.size(); ++index) {
        if (index > 0) args += ", ";
        args += parameters[index].explain(verbosity);
    }
    return fmt::format("{}({})", NAME, args);
}

LogicalFunction EverNeTbigintTbigintLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
        newChildren.emplace_back(child.withInferredDataType(schema));
    return withChildren(newChildren);
}

SerializableFunction EverNeTbigintTbigintLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEverNeTbigintTbigintLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3,
                 "EverNeTbigintTbigintLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    return EverNeTbigintTbigintLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
