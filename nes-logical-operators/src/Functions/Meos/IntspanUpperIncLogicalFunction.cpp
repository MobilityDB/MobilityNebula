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

#include <Functions/Meos/IntspanUpperIncLogicalFunction.hpp>
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

IntspanUpperIncLogicalFunction::IntspanUpperIncLogicalFunction(LogicalFunction sp)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(1);
    parameters.push_back(std::move(sp));
}
DataType IntspanUpperIncLogicalFunction::getDataType() const { return dataType; }
LogicalFunction IntspanUpperIncLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> IntspanUpperIncLogicalFunction::getChildren() const { return parameters; }
LogicalFunction IntspanUpperIncLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==1,"IntspanUpperIncLogicalFunction requires 1 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view IntspanUpperIncLogicalFunction::getType() const { return NAME; }
bool IntspanUpperIncLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const IntspanUpperIncLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string IntspanUpperIncLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction IntspanUpperIncLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(1);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "sp must be VARSIZED");
    return withChildren(c);
}
SerializableFunction IntspanUpperIncLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterIntspanUpperIncLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==1,
                 "IntspanUpperIncLogicalFunction requires 1 children but got {}",
                 arguments.children.size());
    return IntspanUpperIncLogicalFunction(
                                 std::move(arguments.children[0]));
}

} // namespace NES
