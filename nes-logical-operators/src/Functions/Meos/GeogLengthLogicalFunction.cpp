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

#include <Functions/Meos/GeogLengthLogicalFunction.hpp>

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

GeogLengthLogicalFunction::GeogLengthLogicalFunction(LogicalFunction wkt1)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(1);
    parameters.push_back(std::move(wkt1));
}

DataType GeogLengthLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeogLengthLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeogLengthLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeogLengthLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 1, "GeogLengthLogicalFunction requires 1 child, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeogLengthLogicalFunction::getType() const { return NAME; }

bool GeogLengthLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeogLengthLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeogLengthLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeogLengthLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> newChildren;
    newChildren.reserve(1);
    newChildren.emplace_back(parameters[0].withInferredDataType(schema));
    INVARIANT(newChildren[0].getDataType().isType(DataType::Type::VARSIZED),
              "wkt1 must be VARSIZED, but was: {}", newChildren[0].getDataType());
    return withChildren(newChildren);
}

SerializableFunction GeogLengthLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeogLengthLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 1,
                 "GeogLengthLogicalFunction requires 1 child but got {}",
                 arguments.children.size());
    return GeogLengthLogicalFunction(std::move(arguments.children[0]));
}

} // namespace NES
