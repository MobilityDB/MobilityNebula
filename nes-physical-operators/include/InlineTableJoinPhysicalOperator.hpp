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
#include <utility>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Passes each record on once per row of a table of values, the values of the row in the table's fields. The values are
/// computed once for the record, row after row, and one loop passes a record of its own on for each row.
class InlineTableJoinPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    InlineTableJoinPhysicalOperator(std::vector<PhysicalFunction> values, std::vector<Record::RecordFieldIdentifier> fields)
        : values(std::move(values)), fields(std::move(fields)) { };
    void execute(ExecutionContext& ctx, Record& record) const override;

    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    /// The values of the table, row after row
    const std::vector<PhysicalFunction> values;
    const std::vector<Record::RecordFieldIdentifier> fields;
    std::optional<PhysicalOperator> child;
};
}
