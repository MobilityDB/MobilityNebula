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

#include <Functions/Meos/Th3indexCellToCenterChildNextLogicalFunction.hpp>

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

Th3indexCellToCenterChildNextLogicalFunction::Th3indexCellToCenterChildNextLogicalFunction(LogicalFunction cell, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(2);
    parameters.push_back(std::move(cell));
    parameters.push_back(std::move(ts));
}

DataType Th3indexCellToCenterChildNextLogicalFunction::getDataType() const { return dataType; }

LogicalFunction Th3indexCellToCenterChildNextLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> Th3indexCellToCenterChildNextLogicalFunction::getChildren() const { return parameters; }

LogicalFunction Th3indexCellToCenterChildNextLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2,
                 "Th3indexCellToCenterChildNextLogicalFunction requires 2 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view Th3indexCellToCenterChildNextLogicalFunction::getType() const { return NAME; }

bool Th3indexCellToCenterChildNextLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const Th3indexCellToCenterChildNextLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string Th3indexCellToCenterChildNextLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction Th3indexCellToCenterChildNextLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(2);
    c.emplace_back(parameters[0].withInferredDataType(schema));
    c.emplace_back(parameters[1].withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "cell must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::UINT64), "ts must be UINT64");
    return withChildren(c);
}

SerializableFunction Th3indexCellToCenterChildNextLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTh3indexCellToCenterChildNextLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2,
                 "Th3indexCellToCenterChildNextLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return Th3indexCellToCenterChildNextLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]));
}

} // namespace NES
