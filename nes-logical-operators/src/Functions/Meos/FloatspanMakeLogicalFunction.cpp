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

#include <Functions/Meos/FloatspanMakeLogicalFunction.hpp>
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

FloatspanMakeLogicalFunction::FloatspanMakeLogicalFunction(LogicalFunction lower, LogicalFunction upper, LogicalFunction lower_inc, LogicalFunction upper_inc)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(4);
    parameters.push_back(std::move(lower));
    parameters.push_back(std::move(upper));
    parameters.push_back(std::move(lower_inc));
    parameters.push_back(std::move(upper_inc));
}
DataType FloatspanMakeLogicalFunction::getDataType() const { return dataType; }
LogicalFunction FloatspanMakeLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> FloatspanMakeLogicalFunction::getChildren() const { return parameters; }
LogicalFunction FloatspanMakeLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==4,"FloatspanMakeLogicalFunction requires 4 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view FloatspanMakeLogicalFunction::getType() const { return NAME; }
bool FloatspanMakeLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const FloatspanMakeLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string FloatspanMakeLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction FloatspanMakeLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(4);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64), "lower must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "upper must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "lower_inc must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "upper_inc must be FLOAT64");
    return withChildren(c);
}
SerializableFunction FloatspanMakeLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterFloatspanMakeLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==4,
                 "FloatspanMakeLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    return FloatspanMakeLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]));
}

} // namespace NES
