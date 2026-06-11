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

#include <Functions/Meos/AlwaysGtTemporalTemporalLogicalFunction.hpp>

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

AlwaysGtTemporalTemporalLogicalFunction::AlwaysGtTemporalTemporalLogicalFunction(LogicalFunction valueA,
                                          LogicalFunction tsA,
                                          LogicalFunction valueB,
                                          LogicalFunction tsB)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::INT32))
{
    parameters.reserve(4);
    parameters.push_back(std::move(valueA));
    parameters.push_back(std::move(tsA));
    parameters.push_back(std::move(valueB));
    parameters.push_back(std::move(tsB));
}

DataType AlwaysGtTemporalTemporalLogicalFunction::getDataType() const
{
    return dataType;
}

LogicalFunction AlwaysGtTemporalTemporalLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this;
    copy.dataType = newDataType;
    return copy;
}

std::vector<LogicalFunction> AlwaysGtTemporalTemporalLogicalFunction::getChildren() const
{
    return parameters;
}

LogicalFunction AlwaysGtTemporalTemporalLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4, "AlwaysGtTemporalTemporalLogicalFunction requires 4 children, but got {}", children.size());
    auto copy = *this;
    copy.parameters = children;
    return copy;
}

std::string_view AlwaysGtTemporalTemporalLogicalFunction::getType() const
{
    return NAME;
}

bool AlwaysGtTemporalTemporalLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const AlwaysGtTemporalTemporalLogicalFunction*>(&rhs))
    {
        return parameters == other->parameters;
    }
    return false;
}

std::string AlwaysGtTemporalTemporalLogicalFunction::explain(ExplainVerbosity verbosity) const
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

LogicalFunction AlwaysGtTemporalTemporalLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(parameters.size());
    for (const auto& child : parameters)
    {
        newChildren.emplace_back(child.withInferredDataType(schema));
    }
    return withChildren(newChildren);
}

SerializableFunction AlwaysGtTemporalTemporalLogicalFunction::serialize() const
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

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterAlwaysGtTemporalTemporalLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 4,
                 "AlwaysGtTemporalTemporalLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    auto arg0 = std::move(arguments.children[0]);
    auto arg1 = std::move(arguments.children[1]);
    auto arg2 = std::move(arguments.children[2]);
    auto arg3 = std::move(arguments.children[3]);
    return AlwaysGtTemporalTemporalLogicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
