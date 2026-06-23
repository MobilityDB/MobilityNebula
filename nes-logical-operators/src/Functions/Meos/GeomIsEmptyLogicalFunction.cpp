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

#include <Functions/Meos/GeomIsEmptyLogicalFunction.hpp>

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

GeomIsEmptyLogicalFunction::GeomIsEmptyLogicalFunction(LogicalFunction wkt1)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(1);
    parameters.push_back(std::move(wkt1));
}

DataType GeomIsEmptyLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeomIsEmptyLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeomIsEmptyLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeomIsEmptyLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "GeomIsEmptyLogicalFunction requires 1 child, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeomIsEmptyLogicalFunction::getType() const { return NAME; }

bool GeomIsEmptyLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeomIsEmptyLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeomIsEmptyLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeomIsEmptyLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(1);
    newChildren.emplace_back(parameters[0].withInferredDataType(schema));
    INVARIANT(newChildren[0].getDataType().isType(DataType::Type::VARSIZED),
              "wkt1 must be VARSIZED, but was: {}", newChildren[0].getDataType());
    return withChildren(newChildren);
}

SerializableFunction GeomIsEmptyLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeomIsEmptyLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1,
                 "GeomIsEmptyLogicalFunction requires 1 child but got {}",
                 arguments.children.size());
    return GeomIsEmptyLogicalFunction(std::move(arguments.children[0]));
}

} // namespace NES
