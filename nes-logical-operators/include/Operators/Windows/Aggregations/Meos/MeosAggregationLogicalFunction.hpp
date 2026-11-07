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
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// One overload of a MEOS aggregate: the data type of its argument and of its result, named as the
/// DataType registry names them (TINT, TGEOMPOINT, ...), and the physical aggregation computing it.
struct MeosAggregateOverload
{
    std::string_view argument;
    std::string_view result;
    std::string_view physicalAggregation;
};

/// A MEOS aggregate called by its name, as the MEOS catalog's `aggregates` section states it (tCount,
/// mergeAgg, extent, ...). The aggregate is one name over several overloads, as in MobilityDB; the
/// overload is chosen from the data type of the field once it is inferred, and names the physical
/// aggregation lowering it. The names and their overloads are generated from the catalog by
/// tools/codegen/codegen_nebula_sql.py.
class MeosAggregationLogicalFunction final : public WindowAggregationLogicalFunction
{
public:
    MeosAggregationLogicalFunction(
        std::string_view name,
        std::span<const MeosAggregateOverload> overloads,
        const FieldAccessLogicalFunction& onField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    [[nodiscard]] SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    /// The physical aggregation of the chosen overload, else the name
    [[nodiscard]] std::string_view getPhysicalName() const noexcept override;

private:
    /// The overload whose argument type the field's is, else the first one whose numeric argument the
    /// field's number can be cast to; none while the field's type is not yet inferred
    [[nodiscard]] std::optional<size_t> chooseOverload() const;

    std::string_view name;
    std::span<const MeosAggregateOverload> overloads;
    std::optional<size_t> chosen;
};

}
