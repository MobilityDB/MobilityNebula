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

#include <Functions/Meos/GeomShortestline2dLogicalFunction.hpp>

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

GeomShortestline2dLogicalFunction::GeomShortestline2dLogicalFunction(LogicalFunction wkt1, LogicalFunction wkt2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(2);
    parameters.push_back(std::move(wkt1));
    parameters.push_back(std::move(wkt2));
}

DataType GeomShortestline2dLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeomShortestline2dLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeomShortestline2dLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeomShortestline2dLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2,
                 "GeomShortestline2dLogicalFunction requires 2 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeomShortestline2dLogicalFunction::getType() const { return NAME; }

bool GeomShortestline2dLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeomShortestline2dLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeomShortestline2dLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeomShortestline2dLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(2);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt1 must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::VARSIZED), "wkt2 must be VARSIZED");
    return withChildren(c);
}

SerializableFunction GeomShortestline2dLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeomShortestline2dLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2,
                 "GeomShortestline2dLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return GeomShortestline2dLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]));
}

} // namespace NES
