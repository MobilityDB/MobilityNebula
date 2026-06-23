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

#pragma once
#include <Functions/LogicalFunction.hpp>
#include <string_view>
#include <vector>

namespace NES {

class TleTextTtextLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "TleTextTtext";

    TleTextTtextLogicalFunction(LogicalFunction value, LogicalFunction ref, LogicalFunction ts);

    DataType                     getDataType()                     const override;
    LogicalFunction              withDataType(const DataType&)     const override;
    std::vector<LogicalFunction> getChildren()                     const override;
    LogicalFunction              withChildren(const std::vector<LogicalFunction>&) const override;
    std::string_view             getType()                         const override;
    bool                         operator==(const LogicalFunctionConcept&) const override;
    std::string                  explain(ExplainVerbosity)         const override;
    LogicalFunction              withInferredDataType(const Schema&) const override;
    SerializableFunction         serialize()                       const override;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
};

} // namespace NES
