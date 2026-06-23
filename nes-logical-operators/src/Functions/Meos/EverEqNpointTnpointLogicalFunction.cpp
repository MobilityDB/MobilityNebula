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

#include <Functions/Meos/EverEqNpointTnpointLogicalFunction.hpp>

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

EverEqNpointTnpointLogicalFunction::EverEqNpointTnpointLogicalFunction(LogicalFunction rid_np, LogicalFunction pos_np, LogicalFunction rid_tp, LogicalFunction pos_tp, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(5);
    parameters.push_back(std::move(rid_np));
    parameters.push_back(std::move(pos_np));
    parameters.push_back(std::move(rid_tp));
    parameters.push_back(std::move(pos_tp));
    parameters.push_back(std::move(ts));
}

DataType EverEqNpointTnpointLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EverEqNpointTnpointLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EverEqNpointTnpointLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EverEqNpointTnpointLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5,
                 "EverEqNpointTnpointLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EverEqNpointTnpointLogicalFunction::getType() const { return NAME; }

bool EverEqNpointTnpointLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EverEqNpointTnpointLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EverEqNpointTnpointLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction EverEqNpointTnpointLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(5);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "rid_np must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "pos_np must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "rid_tp must be UINT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "pos_tp must be FLOAT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    return withChildren(c);
}

SerializableFunction EverEqNpointTnpointLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEverEqNpointTnpointLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "EverEqNpointTnpointLogicalFunction requires 5 children but got {}",
                 arguments.children.size());
    return EverEqNpointTnpointLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]));
}

} // namespace NES
