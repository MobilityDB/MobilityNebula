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

#include <Functions/Meos/EverEqTnpointNpointLogicalFunction.hpp>

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

EverEqTnpointNpointLogicalFunction::EverEqTnpointNpointLogicalFunction(LogicalFunction rid1, LogicalFunction pos1, LogicalFunction ts, LogicalFunction rid2, LogicalFunction pos2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(5);
    parameters.push_back(std::move(rid1));
    parameters.push_back(std::move(pos1));
    parameters.push_back(std::move(ts));
    parameters.push_back(std::move(rid2));
    parameters.push_back(std::move(pos2));
}

DataType EverEqTnpointNpointLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EverEqTnpointNpointLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EverEqTnpointNpointLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EverEqTnpointNpointLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5,
                 "EverEqTnpointNpointLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EverEqTnpointNpointLogicalFunction::getType() const { return NAME; }

bool EverEqTnpointNpointLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EverEqTnpointNpointLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EverEqTnpointNpointLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction EverEqTnpointNpointLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(5);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "rid1 must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "pos1 must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64), "rid2 must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::FLOAT64), "pos2 must be FLOAT64");
    return withChildren(c);
}

SerializableFunction EverEqTnpointNpointLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEverEqTnpointNpointLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "EverEqTnpointNpointLogicalFunction requires 5 children but got {}",
                 arguments.children.size());
    return EverEqTnpointNpointLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]));
}

} // namespace NES
