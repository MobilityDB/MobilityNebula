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

#include <Functions/Meos/JsonbArrayElementTextLogicalFunction.hpp>
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

JsonbArrayElementTextLogicalFunction::JsonbArrayElementTextLogicalFunction(LogicalFunction jb, LogicalFunction idx)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(2);
    parameters.push_back(std::move(jb));
    parameters.push_back(std::move(idx));
}
DataType JsonbArrayElementTextLogicalFunction::getDataType() const { return dataType; }
LogicalFunction JsonbArrayElementTextLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> JsonbArrayElementTextLogicalFunction::getChildren() const { return parameters; }
LogicalFunction JsonbArrayElementTextLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==2,"JsonbArrayElementTextLogicalFunction requires 2 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view JsonbArrayElementTextLogicalFunction::getType() const { return NAME; }
bool JsonbArrayElementTextLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const JsonbArrayElementTextLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string JsonbArrayElementTextLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction JsonbArrayElementTextLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(2);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "jb must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "idx must be FLOAT64");
    return withChildren(c);
}
SerializableFunction JsonbArrayElementTextLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterJsonbArrayElementTextLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==2,
                 "JsonbArrayElementTextLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return JsonbArrayElementTextLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]));
}

} // namespace NES
