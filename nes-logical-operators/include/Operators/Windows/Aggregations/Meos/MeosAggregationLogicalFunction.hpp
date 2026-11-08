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

#include <cstddef>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// One overload of a MEOS aggregate: the data type of its argument, those of the query literals following
/// it and of its result, named as the DataType registry names them (TINT, TGEOMPOINT, FLOAT64, VARSIZED,
/// ...), and the physical aggregation computing it.
struct MeosAggregateOverload
{
    std::string_view argument;
    std::span<const std::string_view> literals;
    std::string_view result;
    std::string_view physicalAggregation;
};

/// A MEOS aggregate called by its name, as the MEOS catalog's `aggregates` section states it (tCount,
/// mergeAgg, wCount, appendInstantAgg, ...), over a field and the query literals following it (the
/// interval of wCount, the interpolation of appendInstantAgg). The aggregate is one name over several
/// overloads, as in MobilityDB; the overload is chosen from the data type of the field once it is
/// inferred and from the literals, and names the physical aggregation lowering it. The names and their
/// overloads are generated from the catalog by tools/codegen/codegen_nebula_sql.py.
class MeosAggregationLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    MeosAggregationLogicalFunction(
        std::string_view name,
        std::span<const MeosAggregateOverload> overloads,
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField,
        std::vector<std::string> literals);

    void inferStamp(const Schema& schema) override;
    [[nodiscard]] SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    /// The physical aggregation of the chosen overload, else the name
    [[nodiscard]] std::string_view getPhysicalName() const noexcept override;
    [[nodiscard]] std::vector<std::string> getLiterals() const override;

private:
    /// The overload whose argument type the field's is and whose literal types the literals' text reads
    /// as, else the first one whose numeric argument the field's number can be cast to; none while the
    /// field's type is not yet inferred
    [[nodiscard]] std::optional<size_t> chooseOverload() const;

    std::string_view name;
    std::span<const MeosAggregateOverload> overloads;
    std::vector<std::string> literals;
    std::optional<size_t> chosen;
};

}
