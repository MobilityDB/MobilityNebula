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
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>
#include <val.hpp>
#include <val_ptr.hpp>

namespace NES
{

void UnnestPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    const auto value = array.execute(record, ctx.pipelineMemoryProvider.arena).cast<VariableSizedData>();
    /// An array carried in no bytes is no value, and gives no row
    if (value.getContentSize() > nautilus::val<uint32_t>(0))
    {
        const auto count = Nautilus::Util::readValueFromMemRef<uint32_t>(value.getContent());
        nautilus::val<int8_t*> element = value.getContent() + nautilus::val<uint64_t>(sizeof(uint32_t));
        for (nautilus::val<uint32_t> i = 0; i < count; i = i + nautilus::val<uint32_t>(1))
        {
            /// A record of its own for each element, as a probe builds the records it passes on
            const VariableSizedData elementValue(element);
            Record unnested;
            unnested.reassignFields(record);
            unnested.write(elementFieldName, elementValue);
            executeChild(ctx, unnested);
            element = element + nautilus::val<uint64_t>(sizeof(uint32_t)) + elementValue.getContentSize();
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
