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

#include <Functions/Meos/GeoAsGeojsonLogicalFunction.hpp>

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

GeoAsGeojsonLogicalFunction::GeoAsGeojsonLogicalFunction(LogicalFunction wkt, LogicalFunction option, LogicalFunction precision)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(3);
    parameters.push_back(std::move(wkt));
    parameters.push_back(std::move(option));
    parameters.push_back(std::move(precision));
}

DataType GeoAsGeojsonLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeoAsGeojsonLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeoAsGeojsonLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeoAsGeojsonLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 3,
                 "GeoAsGeojsonLogicalFunction requires 3 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeoAsGeojsonLogicalFunction::getType() const { return NAME; }

bool GeoAsGeojsonLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeoAsGeojsonLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeoAsGeojsonLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeoAsGeojsonLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(3);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "option must be UINT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "precision must be UINT64");
    return withChildren(c);
}

SerializableFunction GeoAsGeojsonLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeoAsGeojsonLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 3,
                 "GeoAsGeojsonLogicalFunction requires 3 children but got {}",
                 arguments.children.size());
    return GeoAsGeojsonLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]));
}

} // namespace NES
