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
#include <functional>
#include <string>
#include <string_view>

#include <DataTypes/DataType.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
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

/// ARRAY_AGG(x): the non-null values of x in a window, in the order the window met them, as the array of the type of x a
/// plugin declares (ExtensionType::element): their number, then each value after its length, the array CROSS JOIN
/// UNNEST reads. ARRAY_AGG over a TGEOMPOINT answers a TGEOMPOINT_ARRAY; a type of no array is refused.
class ArrayAggregationLogicalFunction
{
public:
    explicit ArrayAggregationLogicalFunction(AggregationFieldAccess inputFunction);

    [[nodiscard]] ArrayAggregationLogicalFunction withInferredType(const Schema<Field, Unordered>& schema) const;
    [[nodiscard]] static std::string_view getName() noexcept;
    [[nodiscard]] DataType getAggregateType() const;
    [[nodiscard]] static bool shallIncludeNullValues() noexcept;
    [[nodiscard]] AggregationFieldAccess getInputFunction() const;
    [[nodiscard]] std::string explain(ExplainVerbosity verbosity) const;
    [[nodiscard]] bool operator==(const ArrayAggregationLogicalFunction& other) const;

    static AggregationLogicalFunctionRegistryReturnType create(AggregationLogicalFunctionRegistryArguments arguments);

private:
    AggregationFieldAccess inputFunction;
    static constexpr std::string_view NAME = "Array_Agg";
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::VARSIZED;
};

template <>
struct Reflector<ArrayAggregationLogicalFunction>
{
    Reflected operator()(const ArrayAggregationLogicalFunction& function, const ReflectionContext& context) const;
};

template <>
struct Unreflector<ArrayAggregationLogicalFunction>
{
    ArrayAggregationLogicalFunction operator()(const Reflected& reflected, const ReflectionContext& context) const;
};
}

template <>
struct std::hash<NES::ArrayAggregationLogicalFunction>
{
    size_t operator()(const NES::ArrayAggregationLogicalFunction& aggregationFunction) const noexcept;
};

static_assert(NES::WindowAggregationFunctionConcept<NES::ArrayAggregationLogicalFunction>);
