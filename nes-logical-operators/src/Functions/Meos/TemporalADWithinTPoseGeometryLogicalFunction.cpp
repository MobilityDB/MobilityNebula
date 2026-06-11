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

#include <Functions/Meos/TemporalADWithinTPoseGeometryLogicalFunction.hpp>

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

TemporalADWithinTPoseGeometryLogicalFunction::TemporalADWithinTPoseGeometryLogicalFunction(LogicalFunction x,
                                          LogicalFunction y,
                                          LogicalFunction theta,
                                          LogicalFunction timestamp,
                                          LogicalFunction geometry,
                                          LogicalFunction dist)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(6);
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
    parameters.push_back(std::move(theta));
    parameters.push_back(std::move(timestamp));
    parameters.push_back(std::move(geometry));
    parameters.push_back(std::move(dist));
}

DataType TemporalADWithinTPoseGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalADWithinTPoseGeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalADWithinTPoseGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalADWithinTPoseGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 6, "TemporalADWithinTPoseGeometryLogicalFunction requires 6 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view TemporalADWithinTPoseGeometryLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalADWithinTPoseGeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalADWithinTPoseGeometryLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string TemporalADWithinTPoseGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction TemporalADWithinTPoseGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
}

SerializableFunction TemporalADWithinTPoseGeometryLogicalFunction::serialize() const
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

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalADWithinTPoseGeometryLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 6,
                 "TemporalADWithinTPoseGeometryLogicalFunction requires 6 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    auto arg3 = std::move(arguments.children[3]);
    auto arg4 = std::move(arguments.children[4]);
    auto arg5 = std::move(arguments.children[5]);
    return TemporalADWithinTPoseGeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5));
}

} // namespace NES
