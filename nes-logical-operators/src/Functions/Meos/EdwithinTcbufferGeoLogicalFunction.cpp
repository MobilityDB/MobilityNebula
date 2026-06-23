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

#include <Functions/Meos/EdwithinTcbufferGeoLogicalFunction.hpp>

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

EdwithinTcbufferGeoLogicalFunction::EdwithinTcbufferGeoLogicalFunction(LogicalFunction lon, LogicalFunction lat,
                                            LogicalFunction radius, LogicalFunction ts,
                                            LogicalFunction wkt, LogicalFunction dist)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(6);
    parameters.push_back(std::move(lon));
    parameters.push_back(std::move(lat));
    parameters.push_back(std::move(radius));
    parameters.push_back(std::move(ts));
    parameters.push_back(std::move(wkt));
    parameters.push_back(std::move(dist));
}

DataType EdwithinTcbufferGeoLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EdwithinTcbufferGeoLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EdwithinTcbufferGeoLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EdwithinTcbufferGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 6,
                 "EdwithinTcbufferGeoLogicalFunction requires 6 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EdwithinTcbufferGeoLogicalFunction::getType() const { return NAME; }

bool EdwithinTcbufferGeoLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EdwithinTcbufferGeoLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EdwithinTcbufferGeoLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction EdwithinTcbufferGeoLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(6);
    c.emplace_back(parameters[0].withInferredDataType(schema));
    c.emplace_back(parameters[1].withInferredDataType(schema));
    c.emplace_back(parameters[2].withInferredDataType(schema));
    c.emplace_back(parameters[3].withInferredDataType(schema));
    c.emplace_back(parameters[4].withInferredDataType(schema));
    c.emplace_back(parameters[5].withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64),  "lon must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64),  "lat must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64),  "radius must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64),   "ts must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARCHAR");
    INVARIANT(c[5].getDataType().isType(DataType::Type::FLOAT64),  "dist must be FLOAT64");
    return withChildren(c);
}

SerializableFunction EdwithinTcbufferGeoLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEdwithinTcbufferGeoLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 6,
                 "EdwithinTcbufferGeoLogicalFunction requires 6 children but got {}",
                 arguments.children.size());
    return EdwithinTcbufferGeoLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]));
}

} // namespace NES
