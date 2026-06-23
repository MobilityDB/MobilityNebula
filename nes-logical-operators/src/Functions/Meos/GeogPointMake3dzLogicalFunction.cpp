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

#include <Functions/Meos/GeogPointMake3dzLogicalFunction.hpp>

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

GeogPointMake3dzLogicalFunction::GeogPointMake3dzLogicalFunction(LogicalFunction srid, LogicalFunction x, LogicalFunction y, LogicalFunction z)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::VARSIZED))
{
    parameters.reserve(4);
    parameters.push_back(std::move(srid));
    parameters.push_back(std::move(x));
    parameters.push_back(std::move(y));
    parameters.push_back(std::move(z));
}

DataType GeogPointMake3dzLogicalFunction::getDataType() const { return dataType; }

LogicalFunction GeogPointMake3dzLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> GeogPointMake3dzLogicalFunction::getChildren() const { return parameters; }

LogicalFunction GeogPointMake3dzLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 4,
                 "GeogPointMake3dzLogicalFunction requires 4 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view GeogPointMake3dzLogicalFunction::getType() const { return NAME; }

bool GeogPointMake3dzLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const GeogPointMake3dzLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string GeogPointMake3dzLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction GeogPointMake3dzLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(4);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::UINT64), "srid must be UINT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "x must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "y must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::FLOAT64), "z must be FLOAT64");
    return withChildren(c);
}

SerializableFunction GeogPointMake3dzLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterGeogPointMake3dzLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 4,
                 "GeogPointMake3dzLogicalFunction requires 4 children but got {}",
                 arguments.children.size());
    return GeogPointMake3dzLogicalFunction(
                                 std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]));
}

} // namespace NES
