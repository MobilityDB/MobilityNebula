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

#include <Functions/Meos/NadTposePoseLogicalFunction.hpp>

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

NadTposePoseLogicalFunction::NadTposePoseLogicalFunction(LogicalFunction x, LogicalFunction y, LogicalFunction theta, LogicalFunction ts, LogicalFunction x2, LogicalFunction y2, LogicalFunction theta2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(7);
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
    parameters.push_back(std::move(theta));
    parameters.push_back(std::move(ts));
    parameters.push_back(std::move(x2));
    parameters.push_back(std::move(y2));
    parameters.push_back(std::move(theta2));
}

DataType NadTposePoseLogicalFunction::getDataType() const { return dataType; }

LogicalFunction NadTposePoseLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> NadTposePoseLogicalFunction::getChildren() const { return parameters; }

LogicalFunction NadTposePoseLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 7,
                 "NadTposePoseLogicalFunction requires 7 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view NadTposePoseLogicalFunction::getType() const { return NAME; }

bool NadTposePoseLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const NadTposePoseLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string NadTposePoseLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction NadTposePoseLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(7);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64), "x must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "y must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "theta must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::FLOAT64), "x2 must be FLOAT64");
    INVARIANT(c[5].getDataType().isType(DataType::Type::FLOAT64), "y2 must be FLOAT64");
    INVARIANT(c[6].getDataType().isType(DataType::Type::FLOAT64), "theta2 must be FLOAT64");
    return withChildren(c);
}

SerializableFunction NadTposePoseLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterNadTposePoseLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 7,
                 "NadTposePoseLogicalFunction requires 7 children but got {}",
                 arguments.children.size());
    return NadTposePoseLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]),
                                 std::move(arguments.children[6]));
}

} // namespace NES
