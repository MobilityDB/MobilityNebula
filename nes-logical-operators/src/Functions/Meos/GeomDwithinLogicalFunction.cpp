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

#include <Functions/Meos/GeomDwithinLogicalFunction.hpp>

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

GeomDwithinLogicalFunction::GeomDwithinLogicalFunction(LogicalFunction wkt1, LogicalFunction wkt2,
                                            LogicalFunction tolerance)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(3);
    parameters.push_back(std::move(wkt1));
    parameters.push_back(std::move(wkt2));
    parameters.push_back(std::move(tolerance));
}

DataType GeomDwithinLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeomDwithinLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeomDwithinLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeomDwithinLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3,
                 "GeomDwithinLogicalFunction requires 3 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeomDwithinLogicalFunction::getType() const { return NAME; }

bool GeomDwithinLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeomDwithinLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeomDwithinLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeomDwithinLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(3);
    c.emplace_back(parameters[0].withInferredDataType(schema));
    c.emplace_back(parameters[1].withInferredDataType(schema));
    c.emplace_back(parameters[2].withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt1 must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::VARSIZED), "wkt2 must be VARSIZED");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64),  "tolerance must be FLOAT64");
    return withChildren(c);
}

SerializableFunction GeomDwithinLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeomDwithinLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3,
                 "GeomDwithinLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    return GeomDwithinLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]));
}

} // namespace NES
