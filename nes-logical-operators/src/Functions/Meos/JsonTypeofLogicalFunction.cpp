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

#include <Functions/Meos/JsonTypeofLogicalFunction.hpp>

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

JsonTypeofLogicalFunction::JsonTypeofLogicalFunction(LogicalFunction js)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(1);
    parameters.push_back(std::move(js));
}

DataType JsonTypeofLogicalFunction::getDataType() const { return dataType; }

LogicalFunction JsonTypeofLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> JsonTypeofLogicalFunction::getChildren() const { return parameters; }

LogicalFunction JsonTypeofLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1,
                 "JsonTypeofLogicalFunction requires 1 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view JsonTypeofLogicalFunction::getType() const { return NAME; }

bool JsonTypeofLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const JsonTypeofLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string JsonTypeofLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction JsonTypeofLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(1);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "js must be VARSIZED");
    return withChildren(c);
}

SerializableFunction JsonTypeofLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterJsonTypeofLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1,
                 "JsonTypeofLogicalFunction requires 1 children but got {}",
                 arguments.children.size());
    return JsonTypeofLogicalFunction(
                                 std::move(arguments.children[0]));
}

} // namespace NES
