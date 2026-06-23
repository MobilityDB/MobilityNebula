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

#include <Functions/Meos/AtouchesTrgeometryGeoLogicalFunction.hpp>
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

AtouchesTrgeometryGeoLogicalFunction::AtouchesTrgeometryGeoLogicalFunction(LogicalFunction ref_wkt, LogicalFunction x1, LogicalFunction y1, LogicalFunction theta1, LogicalFunction ts1, LogicalFunction tgt_wkt)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(6);
    parameters.push_back(std::move(ref_wkt));
    parameters.push_back(std::move(x1));
    parameters.push_back(std::move(y1));
    parameters.push_back(std::move(theta1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(tgt_wkt));
}
DataType AtouchesTrgeometryGeoLogicalFunction::getDataType() const { return dataType; }
LogicalFunction AtouchesTrgeometryGeoLogicalFunction::withDataType(const DataType& d) const { auto c=*this; c.dataType=d; return c; }
std::vector<LogicalFunction> AtouchesTrgeometryGeoLogicalFunction::getChildren() const { return parameters; }
LogicalFunction AtouchesTrgeometryGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const {
    PRECONDITION(children.size()==6,"AtouchesTrgeometryGeoLogicalFunction requires 6 children, but got {}",children.size());
    auto c=*this; c.parameters=children; return c;
}
std::string_view AtouchesTrgeometryGeoLogicalFunction::getType() const { return NAME; }
bool AtouchesTrgeometryGeoLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    if (const auto* o=dynamic_cast<const AtouchesTrgeometryGeoLogicalFunction*>(&rhs)) return parameters==o->parameters;
    return false;
}
std::string AtouchesTrgeometryGeoLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("{}({})",NAME,parameters[0].explain(v));
}
LogicalFunction AtouchesTrgeometryGeoLogicalFunction::withInferredDataType(const Schema& schema) const {
    std::vector<LogicalFunction> c; c.reserve(6);
    for (const auto& p : parameters) c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "ref_wkt must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "x1 must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "y1 must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "theta1 must be FLOAT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::UINT64), "ts1 must be UINT64");
    INVARIANT(c[5].getDataType().isType(DataType::Type::VARSIZED), "tgt_wkt must be VARSIZED");
    return withChildren(c);
}
SerializableFunction AtouchesTrgeometryGeoLogicalFunction::serialize() const {
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& ch : parameters) proto.add_children()->CopyFrom(ch.serialize());
    return proto;
}
LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAtouchesTrgeometryGeoLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size()==6,
                 "AtouchesTrgeometryGeoLogicalFunction requires 6 children but got {}",
                 arguments.children.size());
    return AtouchesTrgeometryGeoLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]));
}

} // namespace NES
