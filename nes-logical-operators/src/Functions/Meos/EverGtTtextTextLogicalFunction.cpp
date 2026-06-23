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

#include <Functions/Meos/EverGtTtextTextLogicalFunction.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Serialization/DataTypeSerializationUtil.hpp>
#include <Functions/LogicalFunctionProvider.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Util/PlanRenderer.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace NES {

EverGtTtextTextLogicalFunction::EverGtTtextTextLogicalFunction(
    LogicalFunction value, LogicalFunction ref, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64)) {
    parameters = {std::move(value), std::move(ref), std::move(ts)};
}

DataType EverGtTtextTextLogicalFunction::getDataType() const { return dataType; }

LogicalFunction EverGtTtextTextLogicalFunction::withDataType(const DataType& dt) const {
    auto c = *this; c.dataType = dt; return c;
}

std::vector<LogicalFunction> EverGtTtextTextLogicalFunction::getChildren() const { return parameters; }

LogicalFunction EverGtTtextTextLogicalFunction::withChildren(const std::vector<LogicalFunction>& ch) const {
    PRECONDITION(ch.size() == 3, "EverGtTtextText expects 3 params, got {}", ch.size());
    auto c = *this; c.parameters = ch; return c;
}

std::string_view EverGtTtextTextLogicalFunction::getType() const { return NAME; }

bool EverGtTtextTextLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    const auto* o = dynamic_cast<const EverGtTtextTextLogicalFunction*>(&rhs);
    return o && parameters == o->parameters;
}

std::string EverGtTtextTextLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("EverGtTtextText({}, {}, {})",
        parameters[0].explain(v), parameters[1].explain(v), parameters[2].explain(v));
}

LogicalFunction EverGtTtextTextLogicalFunction::withInferredDataType(const Schema& s) const {
    std::vector<LogicalFunction> ch;
    ch.reserve(3);
    for (auto& p : parameters) ch.push_back(p.withInferredDataType(s));
    auto isStr  = [](const DataType& dt) { return dt.isType(DataType::Type::VARSIZED); };
    auto isTime = [](const DataType& dt) { return dt.isType(DataType::Type::UINT64); };
    INVARIANT(isStr(ch[0].getDataType()) && isStr(ch[1].getDataType()) && isTime(ch[2].getDataType()),
              "EverGtTtextText: expects (VARCHAR, VARCHAR, UINT64)");
    return withChildren(ch);
}

SerializableFunction EverGtTtextTextLogicalFunction::serialize() const {
    SerializableFunction sf;
    sf.set_function_type(std::string(NAME));
    for (auto& p : parameters) *sf.add_children() = p.serialize();
    DataTypeSerializationUtil::serializeDataType(dataType, sf.mutable_data_type());
    return sf;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterEverGtTtextTextLogicalFunction(
    LogicalFunctionRegistryArguments arguments) {
    PRECONDITION(arguments.children.size() == 3,
                 "EverGtTtextText expects 3 params, got {}", arguments.children.size());
    return EverGtTtextTextLogicalFunction(
        arguments.children[0], arguments.children[1], arguments.children[2]);
}

} // namespace NES
