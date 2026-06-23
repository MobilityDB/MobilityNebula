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

#include <Functions/Meos/AintersectsTcbufferGeoLogicalFunction.hpp>

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

AintersectsTcbufferGeoLogicalFunction::AintersectsTcbufferGeoLogicalFunction(LogicalFunction lon, LogicalFunction lat,
                                            LogicalFunction radius, LogicalFunction ts,
                                            LogicalFunction wkt)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(5);
    parameters.push_back(std::move(lon));
    parameters.push_back(std::move(lat));
    parameters.push_back(std::move(radius));
    parameters.push_back(std::move(ts));
    parameters.push_back(std::move(wkt));
}

DataType AintersectsTcbufferGeoLogicalFunction::getDataType() const { return dataType; }

LogicalFunction AintersectsTcbufferGeoLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> AintersectsTcbufferGeoLogicalFunction::getChildren() const { return parameters; }

LogicalFunction AintersectsTcbufferGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5,
                 "AintersectsTcbufferGeoLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view AintersectsTcbufferGeoLogicalFunction::getType() const { return NAME; }

bool AintersectsTcbufferGeoLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const AintersectsTcbufferGeoLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string AintersectsTcbufferGeoLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction AintersectsTcbufferGeoLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(5);
    c.emplace_back(parameters[0].withInferredDataType(schema));
    c.emplace_back(parameters[1].withInferredDataType(schema));
    c.emplace_back(parameters[2].withInferredDataType(schema));
    c.emplace_back(parameters[3].withInferredDataType(schema));
    c.emplace_back(parameters[4].withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64),  "lon must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64),  "lat must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64),  "radius must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64),   "ts must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARCHAR");
    return withChildren(c);
}

SerializableFunction AintersectsTcbufferGeoLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAintersectsTcbufferGeoLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "AintersectsTcbufferGeoLogicalFunction requires 5 children but got {}",
                 arguments.children.size());
    return AintersectsTcbufferGeoLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]));
}

} // namespace NES
