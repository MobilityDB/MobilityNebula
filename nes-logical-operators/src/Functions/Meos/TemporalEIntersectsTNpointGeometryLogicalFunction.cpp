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

#include <Functions/Meos/TemporalEIntersectsTNpointGeometryLogicalFunction.hpp>

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

TemporalEIntersectsTNpointGeometryLogicalFunction::TemporalEIntersectsTNpointGeometryLogicalFunction(LogicalFunction rid,
                                          LogicalFunction fraction,
                                          LogicalFunction timestamp,
                                          LogicalFunction geometry)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(4);
    parameters.push_back(std::move(rid));
    parameters.push_back(std::move(fraction));
    parameters.push_back(std::move(timestamp));
    parameters.push_back(std::move(geometry));
}

DataType TemporalEIntersectsTNpointGeometryLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalEIntersectsTNpointGeometryLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalEIntersectsTNpointGeometryLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalEIntersectsTNpointGeometryLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4, "TemporalEIntersectsTNpointGeometryLogicalFunction requires 4 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view TemporalEIntersectsTNpointGeometryLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalEIntersectsTNpointGeometryLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalEIntersectsTNpointGeometryLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string TemporalEIntersectsTNpointGeometryLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction TemporalEIntersectsTNpointGeometryLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
}

SerializableFunction TemporalEIntersectsTNpointGeometryLogicalFunction::serialize() const
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

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalEIntersectsTNpointGeometryLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 4,
                 "TemporalEIntersectsTNpointGeometryLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    auto arg3 = std::move(arguments.children[3]);
    return TemporalEIntersectsTNpointGeometryLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
