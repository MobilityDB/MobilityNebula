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

#include <Functions/Meos/TemporalEIntersectsTNpointTNpointLogicalFunction.hpp>

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

TemporalEIntersectsTNpointTNpointLogicalFunction::TemporalEIntersectsTNpointTNpointLogicalFunction(LogicalFunction ridA,
                                          LogicalFunction fractionA,
                                          LogicalFunction tsA,
                                          LogicalFunction ridB,
                                          LogicalFunction fractionB,
                                          LogicalFunction tsB)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(6);
    parameters.push_back(std::move(ridA));
    parameters.push_back(std::move(fractionA));
    parameters.push_back(std::move(tsA));
    parameters.push_back(std::move(ridB));
    parameters.push_back(std::move(fractionB));
    parameters.push_back(std::move(tsB));
}

DataType TemporalEIntersectsTNpointTNpointLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction TemporalEIntersectsTNpointTNpointLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> TemporalEIntersectsTNpointTNpointLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction TemporalEIntersectsTNpointTNpointLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 6, "TemporalEIntersectsTNpointTNpointLogicalFunction requires 6 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view TemporalEIntersectsTNpointTNpointLogicalFunction::getType() const
{
    return NAME;
}

bool TemporalEIntersectsTNpointTNpointLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const TemporalEIntersectsTNpointTNpointLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string TemporalEIntersectsTNpointTNpointLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction TemporalEIntersectsTNpointTNpointLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
}

SerializableFunction TemporalEIntersectsTNpointTNpointLogicalFunction::serialize() const
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

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterTemporalEIntersectsTNpointTNpointLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 6,
                 "TemporalEIntersectsTNpointTNpointLogicalFunction requires 6 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    auto arg3 = std::move(arguments.children[3]);
    auto arg4 = std::move(arguments.children[4]);
    auto arg5 = std::move(arguments.children[5]);
    return TemporalEIntersectsTNpointTNpointLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5));
}

} // namespace NES
