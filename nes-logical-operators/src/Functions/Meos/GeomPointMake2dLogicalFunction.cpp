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

#include <Functions/Meos/GeomPointMake2dLogicalFunction.hpp>

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

GeomPointMake2dLogicalFunction::GeomPointMake2dLogicalFunction(LogicalFunction srid, LogicalFunction x, LogicalFunction y)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(3);
    parameters.push_back(std::move(srid));
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
}

DataType GeomPointMake2dLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeomPointMake2dLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeomPointMake2dLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeomPointMake2dLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3,
                 "GeomPointMake2dLogicalFunction requires 3 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeomPointMake2dLogicalFunction::getType() const { return NAME; }

bool GeomPointMake2dLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeomPointMake2dLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeomPointMake2dLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeomPointMake2dLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(3);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "srid must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "x must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "y must be FLOAT64");
    return withChildren(c);
}

SerializableFunction GeomPointMake2dLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeomPointMake2dLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3,
                 "GeomPointMake2dLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    return GeomPointMake2dLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]));
}

} // namespace NES
