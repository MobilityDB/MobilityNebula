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

#include <Functions/Meos/EtouchesTcbufferTcbufferLogicalFunction.hpp>

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

EtouchesTcbufferTcbufferLogicalFunction::EtouchesTcbufferTcbufferLogicalFunction(
    LogicalFunction lon1, LogicalFunction lat1, LogicalFunction r1, LogicalFunction ts1,
    LogicalFunction lon2, LogicalFunction lat2, LogicalFunction r2, LogicalFunction ts2)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64))
{
    parameters.reserve(8);
    parameters.push_back(std::move(lon1));
    parameters.push_back(std::move(lat1));
    parameters.push_back(std::move(r1));
    parameters.push_back(std::move(ts1));
    parameters.push_back(std::move(lon2));
    parameters.push_back(std::move(lat2));
    parameters.push_back(std::move(r2));
    parameters.push_back(std::move(ts2));
}

DataType EtouchesTcbufferTcbufferLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EtouchesTcbufferTcbufferLogicalFunction::withDataType(const DataType& newDataType) const
{
    auto copy = *this; copy.dataType = newDataType; return copy;
}

std::vector<LogicalFunction> EtouchesTcbufferTcbufferLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EtouchesTcbufferTcbufferLogicalFunction::withChildren(const std::vector<LogicalFunction>& children) const
{
    PRECONDITION(children.size() == 8,
                 "EtouchesTcbufferTcbufferLogicalFunction requires 8 children, but got {}", children.size());
    auto copy = *this; copy.parameters = children; return copy;
}

std::string_view EtouchesTcbufferTcbufferLogicalFunction::getType() const { return NAME; }

bool EtouchesTcbufferTcbufferLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const
{
    if (const auto* other = dynamic_cast<const EtouchesTcbufferTcbufferLogicalFunction*>(&rhs))
        return parameters == other->parameters;
    return false;
}

std::string EtouchesTcbufferTcbufferLogicalFunction::explain(ExplainVerbosity verbosity) const
{
    return fmt::format("{}({})", NAME, parameters[0].explain(verbosity));
}

LogicalFunction EtouchesTcbufferTcbufferLogicalFunction::withInferredDataType(const Schema& schema) const
{
    std::vector<LogicalFunction> c;
    c.reserve(8);
    for (const auto& p : parameters)
        c.emplace_back(p.withInferredDataType(schema));
    INVARIANT(c[0].getDataType().isType(DataType::Type::FLOAT64), "lon1 must be FLOAT64");
    INVARIANT(c[1].getDataType().isType(DataType::Type::FLOAT64), "lat1 must be FLOAT64");
    INVARIANT(c[2].getDataType().isType(DataType::Type::FLOAT64), "r1 must be FLOAT64");
    INVARIANT(c[3].getDataType().isType(DataType::Type::UINT64),  "ts1 must be UINT64");
    INVARIANT(c[4].getDataType().isType(DataType::Type::FLOAT64), "lon2 must be FLOAT64");
    INVARIANT(c[5].getDataType().isType(DataType::Type::FLOAT64), "lat2 must be FLOAT64");
    INVARIANT(c[6].getDataType().isType(DataType::Type::FLOAT64), "r2 must be FLOAT64");
    INVARIANT(c[7].getDataType().isType(DataType::Type::UINT64),  "ts2 must be UINT64");
    return withChildren(c);
}

SerializableFunction EtouchesTcbufferTcbufferLogicalFunction::serialize() const
{
    SerializableFunction proto;
    proto.set_function_type(std::string(NAME));
    DataTypeSerializationUtil::serializeDataType(dataType, proto.mutable_data_type());
    for (const auto& child : parameters)
        proto.add_children()->CopyFrom(child.serialize());
    return proto;
}

LogicalFunctionRegistryReturnType LogicalFunctionGeneratedRegistrar::RegisterEtouchesTcbufferTcbufferLogicalFunction(
    LogicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.children.size() == 8,
                 "EtouchesTcbufferTcbufferLogicalFunction requires 8 children but got {}",
                 arguments.children.size());
    return EtouchesTcbufferTcbufferLogicalFunction(std::move(arguments.children[0]),
                                 std::move(arguments.children[1]),
                                 std::move(arguments.children[2]),
                                 std::move(arguments.children[3]),
                                 std::move(arguments.children[4]),
                                 std::move(arguments.children[5]),
                                 std::move(arguments.children[6]),
                                 std::move(arguments.children[7]));
}

} // namespace NES
