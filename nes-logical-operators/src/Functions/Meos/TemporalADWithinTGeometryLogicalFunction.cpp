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

#include <Functions/Meos/TemporalADWithinTGeometryLogicalFunction.hpp>

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

TemporalADWithinTGeometryLogicalFunction::TemporalADWithinTGeometryLogicalFunction(LogicalFunction lonA,
                                          LogicalFunction latA,
                                          LogicalFunction tsA,
                                          LogicalFunction lonB,
                                          LogicalFunction latB,
                                          LogicalFunction tsB,
                                          LogicalFunction dist)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(7);
    parameters.push_back(std::move(lonA));
    parameters.push_back(std::move(latA));
    parameters.push_back(std::move(tsA));
    parameters.push_back(std::move(lonB));
    parameters.push_back(std::move(latB));
    parameters.push_back(std::move(tsB));
    parameters.push_back(std::move(dist));
}

DataType TemporalADWithinTGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalADWithinTGeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalADWithinTGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalADWithinTGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 7, "TemporalADWithinTGeometryLogicalFunction requires 7 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view TemporalADWithinTGeometryLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalADWithinTGeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalADWithinTGeometryLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string TemporalADWithinTGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    std::string args;
    for (size_t index = 0; index < parameters.size(); ++index)
    {
        if (index > 0)
        {
            args += ", ";
        }
        args += parameters[index].explain(verbosity);
    }
    return fmt::format("{}({})", NAME, args);
}

LogicalFunction TemporalADWithinTGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
}

SerializableFunction TemporalADWithinTGeometryLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
    {
        proto.add_children()->CopyFrom(child.serialize());
    }
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalADWithinTGeometryLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 7,
                 "TemporalADWithinTGeometryLogicalFunction requires 7 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    auto arg3 = std::move(arguments.children[3]);
    auto arg4 = std::move(arguments.children[4]);
    auto arg5 = std::move(arguments.children[5]);
    auto arg6 = std::move(arguments.children[6]);
    return TemporalADWithinTGeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6));
}

} // namespace NES
