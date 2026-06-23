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

#include <Functions/Meos/GeogCentroidLogicalFunction.hpp>

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

GeogCentroidLogicalFunction::GeogCentroidLogicalFunction(LogicalFunction wkt, LogicalFunction use_spheroid)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(2);
    parameters.push_back(std::move(wkt));
    parameters.push_back(std::move(use_spheroid));
}

DataType GeogCentroidLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeogCentroidLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeogCentroidLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeogCentroidLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2,
                 "GeogCentroidLogicalFunction requires 2 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeogCentroidLogicalFunction::getType() const { return NAME; }

bool GeogCentroidLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeogCentroidLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeogCentroidLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeogCentroidLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(2);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "use_spheroid must be UINT64");
    return withChildren(c);
}

SerializableFunction GeogCentroidLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeogCentroidLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2,
                 "GeogCentroidLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return GeogCentroidLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]));
}

} // namespace NES
