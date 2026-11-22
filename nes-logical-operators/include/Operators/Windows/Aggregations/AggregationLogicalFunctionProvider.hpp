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
#include <memory>
#include <optional>
#include <string>
#include <vector>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES::AggregationLogicalFunctionProvider
{
/// The aggregation registered under the name over the field and the query-literal constants that follow it, as
/// LogicalFunctionProvider::tryProvide provides a function
std::optional<std::shared_ptr<WindowAggregationLogicalFunction>>
tryProvide(const std::string& aggregationName, const FieldAccessLogicalFunction& onField, std::vector<std::string> literals);
}
