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

#include <Functions/Meos/EverNeTquadbinTquadbinLogicalFunction.hpp>
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

EverNeTquadbinTquadbinLogicalFunction::EverNeTquadbinTquadbinLogicalFunction(LogicalFunction cell1, LogicalFunction ts1, LogicalFunction cell2, LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(4);
    parameters.push_back(std::move(cell1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(cell2));
    parameters.push_back(std::move(ts2));
}
DataType EverNeTquadbinTquadbinLogicalFunction::getDataType() const { return dataType; }
LogicalFunction EverNeTquadbinTquadbinLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> EverNeTquadbinTquadbinLogicalFunction::getChildren() const { return parameters; }
LogicalFunction EverNeTquadbinTquadbinLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==4,"EverNeTquadbinTquadbinLogicalFunction requires 4 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view EverNeTquadbinTquadbinLogicalFunction::getType() const { return NAME; }
bool EverNeTquadbinTquadbinLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const EverNeTquadbinTquadbinLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string EverNeTquadbinTquadbinLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction EverNeTquadbinTquadbinLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(4);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "cell1 must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "ts1 must be UINT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "cell2 must be UINT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64), "ts2 must be UINT64");
    return withChildren(c);
}
SerializableFunction EverNeTquadbinTquadbinLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEverNeTquadbinTquadbinLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==4,
                 "EverNeTquadbinTquadbinLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    return EverNeTquadbinTquadbinLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]));
}

} // namespace NES
