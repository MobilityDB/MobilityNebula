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

#include <Functions/Meos/AlwaysNeTrgeometryTrgeometryLogicalFunction.hpp>

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

AlwaysNeTrgeometryTrgeometryLogicalFunction::AlwaysNeTrgeometryTrgeometryLogicalFunction(LogicalFunction ref1_wkt, LogicalFunction x1, LogicalFunction y1, LogicalFunction theta1, LogicalFunction ts1, LogicalFunction ref2_wkt, LogicalFunction x2, LogicalFunction y2, LogicalFunction theta2, LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(10);
    parameters.push_back(std::move(ref1_wkt));
    parameters.push_back(std::move(x1));
    parameters.push_back(std::move(y1));
    parameters.push_back(std::move(theta1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(ref2_wkt));
    parameters.push_back(std::move(x2));
    parameters.push_back(std::move(y2));
    parameters.push_back(std::move(theta2));
    parameters.push_back(std::move(ts2));
}

DataType AlwaysNeTrgeometryTrgeometryLogicalFunction::getDataType() const { return dataType; }

LogicalFunction AlwaysNeTrgeometryTrgeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> AlwaysNeTrgeometryTrgeometryLogicalFunction::getChildren() const { return parameters; }

LogicalFunction AlwaysNeTrgeometryTrgeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 10,
                 "AlwaysNeTrgeometryTrgeometryLogicalFunction requires 10 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view AlwaysNeTrgeometryTrgeometryLogicalFunction::getType() const { return NAME; }

bool AlwaysNeTrgeometryTrgeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const AlwaysNeTrgeometryTrgeometryLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string AlwaysNeTrgeometryTrgeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction AlwaysNeTrgeometryTrgeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(10);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "ref1_wkt must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "x1 must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "y1 must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "theta1 must be FLOAT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::UINT64), "ts1 must be UINT64");
    INVARIANT(c[5].getDataType().isType(DataType::Type::VARSIZED), "ref2_wkt must be VARSIZED");
    INVARIANT(c[6].getDataType().isType(DataType::Type::FLOAT64), "x2 must be FLOAT64");
    INVARIANT(c[7].getDataType().isType(DataType::Type::FLOAT64), "y2 must be FLOAT64");
    INVARIANT(c[8].getDataType().isType(DataType::Type::FLOAT64), "theta2 must be FLOAT64");
    INVARIANT(c[9].getDataType().isType(DataType::Type::UINT64), "ts2 must be UINT64");
    return withChildren(c);
}

SerializableFunction AlwaysNeTrgeometryTrgeometryLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAlwaysNeTrgeometryTrgeometryLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 10,
                 "AlwaysNeTrgeometryTrgeometryLogicalFunction requires 10 children but got {}",
                 arguments.children.size());
    return AlwaysNeTrgeometryTrgeometryLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]),
                                 std::move(arguments.children[6]),
                                 std::move(arguments.children[7]),
                                 std::move(arguments.children[8]),
                                 std::move(arguments.children[9]));
}

} // namespace NES
