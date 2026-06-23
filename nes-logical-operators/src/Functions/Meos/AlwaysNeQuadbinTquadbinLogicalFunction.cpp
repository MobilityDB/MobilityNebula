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

#include <Functions/Meos/AlwaysNeQuadbinTquadbinLogicalFunction.hpp>
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

AlwaysNeQuadbinTquadbinLogicalFunction::AlwaysNeQuadbinTquadbinLogicalFunction(LogicalFunction cell, LogicalFunction inst_cell, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(3);
    parameters.push_back(std::move(cell));
    parameters.push_back(std::move(inst_cell));
    parameters.push_back(std::move(ts));
}
DataType AlwaysNeQuadbinTquadbinLogicalFunction::getDataType() const { return dataType; }
LogicalFunction AlwaysNeQuadbinTquadbinLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> AlwaysNeQuadbinTquadbinLogicalFunction::getChildren() const { return parameters; }
LogicalFunction AlwaysNeQuadbinTquadbinLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==3,"AlwaysNeQuadbinTquadbinLogicalFunction requires 3 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view AlwaysNeQuadbinTquadbinLogicalFunction::getType() const { return NAME; }
bool AlwaysNeQuadbinTquadbinLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const AlwaysNeQuadbinTquadbinLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string AlwaysNeQuadbinTquadbinLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction AlwaysNeQuadbinTquadbinLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(3);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "cell must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "inst_cell must be UINT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    return withChildren(c);
}
SerializableFunction AlwaysNeQuadbinTquadbinLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAlwaysNeQuadbinTquadbinLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==3,
                 "AlwaysNeQuadbinTquadbinLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    return AlwaysNeQuadbinTquadbinLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]));
}

} // namespace NES
