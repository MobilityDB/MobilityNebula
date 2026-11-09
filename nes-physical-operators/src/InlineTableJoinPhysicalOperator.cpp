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

#include <InlineTableJoinPhysicalOperator.hpp>

#include <cstddef>
#include <cstdint>
#include <optional>
#include <utility>
#include <vector>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>

namespace NES
{

void InlineTableJoinPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// The values of every row, computed once for the record
    std::vector<VarVal> table;
    table.reserve(values.size());
    for (const auto& value : values)
    {
        table.push_back(value.execute(record, ctx.pipelineMemoryProvider.arena));
    }
    /// One loop over the rows, which passes a record of its own for each row on from one place, as a scan passes its
    /// records on; the record of row r takes the values of row r
    const auto numberOfFields = fieldNames.size();
    const auto numberOfRows = values.size() / numberOfFields;
    for (nautilus::val<uint64_t> row = 0; row < nautilus::val<uint64_t>(numberOfRows); row = row + nautilus::val<uint64_t>(1))
    {
        Record joined;
        joined.reassignFields(record);
        for (size_t field = 0; field < numberOfFields; ++field)
        {
            VarVal fieldValue = table[field];
            for (size_t other = 1; other < numberOfRows; ++other)
            {
                if (row == nautilus::val<uint64_t>(other))
                {
                    fieldValue = table[other * numberOfFields + field];
                }
            }
            joined.write(fieldNames[field], fieldValue);
        }
        executeChild(ctx, joined);
    }
}

std::optional<PhysicalOperator> InlineTableJoinPhysicalOperator::getChild() const
{
    return child;
}

void InlineTableJoinPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
