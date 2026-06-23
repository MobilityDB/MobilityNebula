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

#include <Functions/Meos/TltTextTtextLogicalFunction.hpp>
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

TltTextTtextLogicalFunction::TltTextTtextLogicalFunction(
    LogicalFunction value, LogicalFunction ref, LogicalFunction ts)
    : dataType(DataTypeProvider::provideDataType(DataType::Type::FLOAT64)) {
    parameters = {std::move(value), std::move(ref), std::move(ts)};
}

DataType TltTextTtextLogicalFunction::getDataType() const { return dataType; }

LogicalFunction TltTextTtextLogicalFunction::withDataType(const DataType& dt) const {
    auto c = *this; c.dataType = dt; return c;
}

std::vector<LogicalFunction> TltTextTtextLogicalFunction::getChildren() const { return parameters; }

LogicalFunction TltTextTtextLogicalFunction::withChildren(const std::vector<LogicalFunction>& ch) const {
    PRECONDITION(ch.size() == 3, "TltTextTtext expects 3 params, got {}", ch.size());
    auto c = *this; c.parameters = ch; return c;
}

std::string_view TltTextTtextLogicalFunction::getType() const { return NAME; }

bool TltTextTtextLogicalFunction::operator==(const LogicalFunctionConcept& rhs) const {
    const auto* o = dynamic_cast<const TltTextTtextLogicalFunction*>(&rhs);
    return o && parameters == o->parameters;
}

std::string TltTextTtextLogicalFunction::explain(ExplainVerbosity v) const {
    return fmt::format("TltTextTtext({}, {}, {})",
        parameters[0].explain(v), parameters[1].explain(v), parameters[2].explain(v));
}

LogicalFunction TltTextTtextLogicalFunction::withInferredDataType(const Schema& s) const {
    std::vector<LogicalFunction> ch;
    ch.reserve(3);
    for (auto& p : parameters) ch.push_back(p.withInferredDataType(s));
    auto isStr  = [](const DataType& dt) { return dt.isType(DataType::Type::VARSIZED); };
    auto isTime = [](const DataType& dt) { return dt.isType(DataType::Type::UINT64); };
    INVARIANT(isStr(ch[0].getDataType()) && isStr(ch[1].getDataType()) && isTime(ch[2].getDataType()),
              "TltTextTtext: expects (VARCHAR, VARCHAR, UINT64)");
    return withChildren(ch);
}

SerializableFunction TltTextTtextLogicalFunction::serialize() const {
    SerializableFunction sf;
    sf.set_function_type(std::string(NAME));
    for (auto& p : parameters) *sf.add_children() = p.serialize();
    DataTypeSerializationUtil::serializeDataType(dataType, sf.mutable_data_type());
    return sf;
}

LogicalFunctionRegistryReturnType
LogicalFunctionGeneratedRegistrar::RegisterTltTextTtextLogicalFunction(
    LogicalFunctionRegistryArguments arguments) {
    PRECONDITION(arguments.children.size() == 3,
                 "TltTextTtext expects 3 params, got {}", arguments.children.size());
    return TltTextTtextLogicalFunction(
        arguments.children[0], arguments.children[1], arguments.children[2]);
}

} // namespace NES
