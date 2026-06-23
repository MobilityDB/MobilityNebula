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

#include <Functions/Meos/GeogToGeomLogicalFunction.hpp>

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

GeogToGeomLogicalFunction::GeogToGeomLogicalFunction(LogicalFunction wkt)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(1);
    parameters.push_back(std::move(wkt));
}

DataType GeogToGeomLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeogToGeomLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeogToGeomLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeogToGeomLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1,
                 "GeogToGeomLogicalFunction requires 1 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeogToGeomLogicalFunction::getType() const { return NAME; }

bool GeogToGeomLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeogToGeomLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeogToGeomLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeogToGeomLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(1);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARSIZED");
    return withChildren(c);
}

SerializableFunction GeogToGeomLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeogToGeomLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1,
                 "GeogToGeomLogicalFunction requires 1 children but got {}",
                 arguments.children.size());
    return GeogToGeomLogicalFunction(
                                 std::move(arguments.children[0]));
}

} // namespace NES
