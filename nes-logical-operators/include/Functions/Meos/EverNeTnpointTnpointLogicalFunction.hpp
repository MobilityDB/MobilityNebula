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
#include <utility>
#include <vector>

namespace NES {

/**
 * @brief Returns 1.0 if the two tnpoint instants are ever not equal, 0.0 otherwise.
 *
 * Args: (rid1:UINT64, pos1:FLOAT64, ts1:UINT64,
 *        rid2:UINT64, pos2:FLOAT64, ts2:UINT64) -> FLOAT64.
 */
class EverNeTnpointTnpointLogicalFunction : public LogicalFunctionConcept {
public:
    static constexpr std::string_view NAME = "EverNeTnpointTnpoint";

    EverNeTnpointTnpointLogicalFunction(LogicalFunction rid1, LogicalFunction pos1, LogicalFunction ts1,
                          LogicalFunction rid2, LogicalFunction pos2, LogicalFunction ts2);

    DataType getDataType() const override;
    LogicalFunction withDataType(const DataType& dataType) const override;
    std::vector<LogicalFunction> getChildren() const override;
    LogicalFunction withChildren(const std::vector<LogicalFunction>& children) const override;
    std::string_view getType() const override;
    bool operator==(const LogicalFunctionConcept& rhs) const override;
    std::string explain(ExplainVerbosity verbosity) const override;
    LogicalFunction withInferredDataType(const Schema& schema) const override;
    SerializableFunction serialize() const override;

private:
    DataType dataType;
    std::vector<LogicalFunction> parameters;
};

} // namespace NES
