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
#include <cstdint>
#include <functional>
#include <string>
#include <string_view>
#include <DataTypes/DataType.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Schema/Field.hpp>
#include <Schema/Schema.hpp>
#include <Schema/SchemaFwd.hpp>
#include <Util/PlanRenderer.hpp>
#include <Util/Reflection.hpp>
#include <AggregationLogicalFunctionRegistry.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{

/// The variance of the values of a numeric field, as SQL states it: VAR_POP divides the sum of the squared deviations
/// from the mean by the number of values, VAR_SAMP by one less. Either is NULL where it is undefined: VAR_POP over no
/// value, VAR_SAMP over fewer than two.
enum class VarianceKind : uint8_t
{
    Population,
    Sample
};

template <VarianceKind Kind>
class VarianceAggregationLogicalFunction final
{
public:
    explicit VarianceAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] VarianceAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const VarianceAggregationLogicalFunction& other) const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = Kind == VarianceKind::Population ? "Var_Pop" : "Var_Samp";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::FLOAT64;
};

using VarPopAggregationLogicalFunction = VarianceAggregationLogicalFunction<VarianceKind::Population>;
using VarSampAggregationLogicalFunction = VarianceAggregationLogicalFunction<VarianceKind::Sample>;

extern template class VarianceAggregationLogicalFunction<VarianceKind::Population>;
extern template class VarianceAggregationLogicalFunction<VarianceKind::Sample>;

template <VarianceKind Kind>
struct Reflector<VarianceAggregationLogicalFunction<Kind>>
{
    Reflected operator()(const VarianceAggregationLogicalFunction<Kind>& function, const ReflectionContext& context) const;
};

template <VarianceKind Kind>
struct Unreflector<VarianceAggregationLogicalFunction<Kind>>
{
    VarianceAggregationLogicalFunction<Kind> operator()(const Reflected& reflected, const ReflectionContext& context) const;
};

}

template <NES::VarianceKind Kind>
struct std::hash<NES::VarianceAggregationLogicalFunction<Kind>>
{
    size_t operator()(const NES::VarianceAggregationLogicalFunction<Kind>& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::VarPopAggregationLogicalFunction>);
static_assert(NES::WindowAggregationFunctionConcept<NES::VarSampAggregationLogicalFunction>);
