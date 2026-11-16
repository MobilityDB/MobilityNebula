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

#include <UnnestPhysicalOperator.hpp>

#include <cstdint>
#include <optional>
#include <utility>
#include <DataTypes/DataTypesUtil.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

void UnnestPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto value = array.execute(record, ctx.pipelineMemoryProvider.arena);
    const auto arrayData = value.getRawValueAs<VariableSizedData>();
    /// A null array, or an array carried in no bytes, gives no record
    if (not value.isNull() and arrayData.getSize() > nautilus::val<uint64_t>(0))
    {
        const auto count = readValueFromMemRef<uint32_t>(arrayData.getContent());
        nautilus::val<int8_t*> element = arrayData.getContent() + nautilus::val<uint64_t>(sizeof(uint32_t));
        for (nautilus::val<uint32_t> i = 0; i < count; i = i + nautilus::val<uint32_t>(1))
        {
            const auto elementSize = readValueFromMemRef<uint32_t>(element);
            const auto elementBytes = element + nautilus::val<uint64_t>(sizeof(uint32_t));
            /// A record of its own for each element, as a probe builds the records it passes on
            Record unnested;
            unnested.reassignFields(record);
            unnested.write(elementField, VarVal{VariableSizedData{elementBytes, nautilus::val<uint64_t>(elementSize)}});
            executeChild(ctx, unnested);
            element = elementBytes + nautilus::val<uint64_t>(elementSize);
        }
    }
}

std::optional<PhysicalOperator> UnnestPhysicalOperator::getChild() const
{
    return child;
}

void UnnestPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
