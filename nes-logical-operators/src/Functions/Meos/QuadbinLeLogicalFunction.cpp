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

#include <Functions/Meos/QuadbinLeLogicalFunction.hpp>
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

QuadbinLeLogicalFunction::QuadbinLeLogicalFunction(LogicalFunction a, LogicalFunction b)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(2);
    parameters.push_back(std::move(a));
    parameters.push_back(std::move(b));
}
DataType QuadbinLeLogicalFunction::getDataType() const { return dataType; }
LogicalFunction QuadbinLeLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> QuadbinLeLogicalFunction::getChildren() const { return parameters; }
LogicalFunction QuadbinLeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==2,"QuadbinLeLogicalFunction requires 2 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view QuadbinLeLogicalFunction::getType() const { return NAME; }
bool QuadbinLeLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const QuadbinLeLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string QuadbinLeLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction QuadbinLeLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(2);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "a must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "b must be UINT64");
    return withChildren(c);
}
SerializableFunction QuadbinLeLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterQuadbinLeLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==2,
                 "QuadbinLeLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return QuadbinLeLogicalFunction(std::move(arguments.children[0]),
                                std::move(arguments.children[1]));
}

} // namespace NES
