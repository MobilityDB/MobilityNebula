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

#include <Functions/Meos/Th3indexGridDistanceLogicalFunction.hpp>

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

Th3indexGridDistanceLogicalFunction::Th3indexGridDistanceLogicalFunction(LogicalFunction cell1, LogicalFunction ts1,
                                            LogicalFunction cell2, LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(4);
    parameters.push_back(std::move(cell1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(cell2));
    parameters.push_back(std::move(ts2));
}

DataType Th3indexGridDistanceLogicalFunction::getDataType() const { return dataType; }

LogicalFunction Th3indexGridDistanceLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> Th3indexGridDistanceLogicalFunction::getChildren() const { return parameters; }

LogicalFunction Th3indexGridDistanceLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4,
                 "Th3indexGridDistanceLogicalFunction requires 4 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view Th3indexGridDistanceLogicalFunction::getType() const { return NAME; }

bool Th3indexGridDistanceLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const Th3indexGridDistanceLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string Th3indexGridDistanceLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction Th3indexGridDistanceLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(4);
    c.emplace_back(parameters[0].withInferredDataType(schema));
    c.emplace_back(parameters[1].withInferredDataType(schema));
    c.emplace_back(parameters[2].withInferredDataType(schema));
    c.emplace_back(parameters[3].withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "cell1 must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "ts1 must be UINT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::UINT64), "cell2 must be UINT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64), "ts2 must be UINT64");
    return withChildren(c);
}

SerializableFunction Th3indexGridDistanceLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTh3indexGridDistanceLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 4,
                 "Th3indexGridDistanceLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    return Th3indexGridDistanceLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]));
}

} // namespace NES
