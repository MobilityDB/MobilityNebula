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

#include <Functions/Meos/EverNeGeoTrgeometryLogicalFunction.hpp>

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

EverNeGeoTrgeometryLogicalFunction::EverNeGeoTrgeometryLogicalFunction(LogicalFunction tgt_wkt, LogicalFunction ref_wkt, LogicalFunction x, LogicalFunction y, LogicalFunction theta, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(6);
    parameters.push_back(std::move(tgt_wkt));
    parameters.push_back(std::move(ref_wkt));
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
    parameters.push_back(std::move(theta));
    parameters.push_back(std::move(ts));
}

DataType EverNeGeoTrgeometryLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EverNeGeoTrgeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EverNeGeoTrgeometryLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EverNeGeoTrgeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 6,
                 "EverNeGeoTrgeometryLogicalFunction requires 6 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EverNeGeoTrgeometryLogicalFunction::getType() const { return NAME; }

bool EverNeGeoTrgeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EverNeGeoTrgeometryLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EverNeGeoTrgeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction EverNeGeoTrgeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(6);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "tgt_wkt must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::VARSIZED), "ref_wkt must be VARSIZED");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "x must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "y must be FLOAT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::FLOAT64), "theta must be FLOAT64");
    INVARIANT(c[5].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    return withChildren(c);
}

SerializableFunction EverNeGeoTrgeometryLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEverNeGeoTrgeometryLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 6,
                 "EverNeGeoTrgeometryLogicalFunction requires 6 children but got {}",
                 arguments.children.size());
    return EverNeGeoTrgeometryLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]));
}

} // namespace NES
