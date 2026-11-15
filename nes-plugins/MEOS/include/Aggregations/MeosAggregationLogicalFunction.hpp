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
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/ReflectionFwd.hpp>

namespace NES
{

/// One overload of a MEOS aggregate: the data type of its argument, those of the constants of the query
/// following it and of its result, named as the DataType registry names them (TINT, TGEOMPOINT, FLOAT64,
/// VARSIZED, ...), and the physical aggregation computing it.
struct MeosAggregateOverload
{
    std::string_view argument;
    std::span<const std::string_view> literals;
    std::string_view result;
    std::string_view physicalAggregation;
};

/// A MEOS aggregate as the catalog's `aggregates` section states it: its name and its overloads, of static
/// storage
struct MeosAggregate
{
    std::string_view name;
    std::span<const MeosAggregateOverload> overloads;
};

/// Every MEOS aggregate the catalog states, in the order of their names, in the table
/// tools/codegen/codegen_nebula_sql.py writes from the catalog
std::span<const MeosAggregate> meosAggregates();

/// The MEOS aggregate of the name, none for a name the catalog does not state
std::optional<MeosAggregate> meosAggregate(std::string_view name);

/// A MEOS aggregate called by its name (tCount, mergeAgg, extent, appendInstantAgg, ...), over a field and the
/// constants of the query following it (the interval of wCount, the interpolation of appendInstantAgg). The
/// aggregate is one name over several overloads, as in MobilityDB; the overload is chosen from the data type
/// of the field once it is inferred and from the constants, and names the physical aggregation lowering it,
/// as MeosLogicalFunction names the physical function of its chosen signature. A MEOS aggregate is STRICT, as
/// in MobilityDB: it skips a null value.
class MeosAggregationLogicalFunction
{
public:
    MeosAggregationLogicalFunction(
        std::string_view name,
        std::span<const MeosAggregateOverload> overloads,
        AggregationFieldAccess inputFunction,
        std::vector<LogicalFunction> literals);

    [[nodiscard]] MeosAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    /// The physical aggregation of the chosen overload, else the name
    [[nodiscard]] std::string_view getName() const noexcept;
    /// The name of the aggregate, as the catalog states it
    [[nodiscard]] std::string_view getAggregateName() const noexcept { return name; }
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept { return false; }
    [[nodiscard]] AggregationFieldAccess getInputFunction() const { return inputFunction; }
    [[nodiscard]] const std::vector<LogicalFunction>& getLiterals() const { return literals; }
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const MeosAggregationLogicalFunction& other) const;

private:
    /// The overload whose argument type the field's is and whose literal types the constants read as, else
    /// the first one whose numeric argument the field's number can be cast to; none while the field's type
    /// is not yet inferred
    [[nodiscard]] std::optional<size_t> chooseOverload(const DataType& fieldType) const;

    std::string_view name;
    std::span<const MeosAggregateOverload> overloads;
    AggregationFieldAccess inputFunction;
    std::vector<LogicalFunction> literals;
    std::optional<size_t> chosen;
};

namespace detail
{
struct ReflectedMeosAggregationLogicalFunction
{
    std::string name;
    AggregationFieldAccess inputFunction;
    std::vector<LogicalFunction> literals;
};
}

template <>
struct Reflector<MeosAggregationLogicalFunction>
{
    Reflected operator()(const MeosAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<MeosAggregationLogicalFunction>
{
    MeosAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <>
struct std::hash<NES::MeosAggregationLogicalFunction>
{
    size_t operator()(const NES::MeosAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::MeosAggregationLogicalFunction>);
