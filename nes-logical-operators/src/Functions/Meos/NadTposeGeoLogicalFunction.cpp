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

#include <Functions/Meos/NadTposeGeoLogicalFunction.hpp>

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

NadTposeGeoLogicalFunction::NadTposeGeoLogicalFunction(LogicalFunction x, LogicalFunction y, LogicalFunction theta, LogicalFunction ts, LogicalFunction wkt)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(5);
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
    parameters.push_back(std::move(theta));
    parameters.push_back(std::move(ts));
    parameters.push_back(std::move(wkt));
}

DataType NadTposeGeoLogicalFunction::getDataType() const { return dataType; }

LogicalFunction NadTposeGeoLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> NadTposeGeoLogicalFunction::getChildren() const { return parameters; }

LogicalFunction NadTposeGeoLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 5,
                 "NadTposeGeoLogicalFunction requires 5 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view NadTposeGeoLogicalFunction::getType() const { return NAME; }

bool NadTposeGeoLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const NadTposeGeoLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string NadTposeGeoLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction NadTposeGeoLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(5);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64), "x must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "y must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "theta must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::VARSIZED), "wkt must be VARSIZED");
    return withChildren(c);
}

SerializableFunction NadTposeGeoLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterNadTposeGeoLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 5,
                 "NadTposeGeoLogicalFunction requires 5 children but got {}",
                 arguments.children.size());
    return NadTposeGeoLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]));
}

} // namespace NES
