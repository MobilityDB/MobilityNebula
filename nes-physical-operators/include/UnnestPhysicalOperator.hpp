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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

/// Passes each record on once per element of the array its function answers, the element in the element field. An array
/// is variable-sized: the number of its elements, then each element as a variable-sized value, its length then its
/// bytes, so the element field refers to the element where the array holds it.
class UnnestPhysicalOperator final : public PhysicalOperatorConcept
{
public:
    UnnestPhysicalOperator(PhysicalFunction array, std::string elementFieldName)
        : array(std::move(array)), elementFieldName(std::move(elementFieldName)) { };
    void execute(ExecutionContext& ctx, Record& record) const override;
    [[nodiscard]] std::optional<PhysicalOperator> getChild() const override;
    void setChild(PhysicalOperator child) override;

private:
    const PhysicalFunction array;
    const std::string elementFieldName;
    std::optional<PhysicalOperator> child;
};

}
