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

#include <Functions/Meos/GeomAzimuthLogicalFunction.hpp>

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

GeomAzimuthLogicalFunction::GeomAzimuthLogicalFunction(LogicalFunction wkt1, LogicalFunction wkt2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(2);
    parameters.push_back(std::move(wkt1));
    parameters.push_back(std::move(wkt2));
}

DataType GeomAzimuthLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeomAzimuthLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeomAzimuthLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeomAzimuthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2, "GeomAzimuthLogicalFunction requires 2 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeomAzimuthLogicalFunction::getType() const { return NAME; }

bool GeomAzimuthLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeomAzimuthLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeomAzimuthLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({}, {})", NAME,
                       parameters[0].explain(verbosity),
                       parameters[1].explain(verbosity));
}

LogicalFunction GeomAzimuthLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(2);
    for (const auto& child : parameters)
        newChildren.emplace_back(child.withInferredDataType(schema));
    INVARIANT(newChildren[0].getDataType().isType(DataType::Type::VARSIZED),
              "wkt1 must be VARSIZED, but was: {}", newChildren[0].getDataType());
    INVARIANT(newChildren[1].getDataType().isType(DataType::Type::VARSIZED),
              "wkt2 must be VARSIZED, but was: {}", newChildren[1].getDataType());
    return withChildren(newChildren);
}

SerializableFunction GeomAzimuthLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeomAzimuthLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2,
                 "GeomAzimuthLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return GeomAzimuthLogicalFunction(std::move(arguments.children[0]),
                                std::move(arguments.children[1]));
}

} // namespace NES
