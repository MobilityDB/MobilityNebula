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

#include <optional>
#include <string>
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Passes each record on once per row of an inline table, the values of the row in its fields. The rows are known when
/// the query is compiled, so the operator passes the records of a row one after the other, a record of its own for each.
class InlineTableJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    InlineTableJoinPhysicalOperator(std::vector<PhysicalFunction> values, std::vector<std::string> fieldNames)
        : values(std::move(values)), fieldNames(std::move(fieldNames)) { };
    void execute(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    const std::vector<PhysicalFunction> values;
    const std::vector<std::string> fieldNames;
    std::optional<PhysicalOperator> child;
};

}
