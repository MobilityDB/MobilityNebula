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

#include <Functions/Meos/LineLocatePointLogicalFunction.hpp>

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

LineLocatePointLogicalFunction::LineLocatePointLogicalFunction(LogicalFunction wkt1, LogicalFunction wkt2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(2);
    parameters.push_back(std::move(wkt1));
    parameters.push_back(std::move(wkt2));
}

DataType LineLocatePointLogicalFunction::getDataType() const { return dataType; }

LogicalFunction LineLocatePointLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> LineLocatePointLogicalFunction::getChildren() const { return parameters; }

LogicalFunction LineLocatePointLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 2,
                 "LineLocatePointLogicalFunction requires 2 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view LineLocatePointLogicalFunction::getType() const { return NAME; }

bool LineLocatePointLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const LineLocatePointLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string LineLocatePointLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction LineLocatePointLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(2);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::VARSIZED), "wkt1 must be VARSIZED");
    INVARIANT(c[1].getDataType().isType(DataType::Type::VARSIZED), "wkt2 must be VARSIZED");
    return withChildren(c);
}

SerializableFunction LineLocatePointLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterLineLocatePointLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 2,
                 "LineLocatePointLogicalFunction requires 2 children but got {}",
                 arguments.children.size());
    return LineLocatePointLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]));
}

} // namespace NES
