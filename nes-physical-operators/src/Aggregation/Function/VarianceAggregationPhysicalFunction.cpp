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

#include <Aggregation/Function/VarianceAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Operators/Windows/Aggregations/VarianceAggregationLogicalFunction.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <nautilus/std/cstring.h>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ExecutionContext.hpp>
#include <val.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// The state holds N, Sx and Sxx as doubles, in this order
constexpr uint64_t offsetN = 0;
constexpr uint64_t offsetSx = sizeof(double);
constexpr uint64_t offsetSxx = 2 * sizeof(double);

nautilus::val<double> readState(const nautilus::val<AggregationState*>& aggregationState, const uint64_t offset)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{offset};
    return VarVal::readNonNullableVarValFromMemory(memArea, DataType{DataType::Type::FLOAT64, DataType::NULLABLE::NOT_NULLABLE})
        .getRawValueAs<nautilus::val<double>>();
}

nautilus::val<double> asDouble(const VarVal& value)
{
    return value.castToType(DataType::Type::FLOAT64).getRawValueAs<nautilus::val<double>>();
}

void writeState(const nautilus::val<AggregationState*>& aggregationState, const uint64_t offset, const nautilus::val<double>& value)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>{offset};
    VarVal{value}.writeToMemory(memArea);
}

/// float8_accum: N and Sx grow by one value and Sxx by the update of Youngs and Cramer. The first value leaves Sxx at
/// x - x, which is zero for a finite x and NaN for a NaN or an infinite one, as PostgreSQL forces it.
void accumulate(const nautilus::val<AggregationState*>& aggregationState, const nautilus::val<double>& value)
{
    const auto count = readState(aggregationState, offsetN);
    const auto sum = readState(aggregationState, offsetSx);
    const auto newCount = count + nautilus::val<double>{1.0};
    const auto newSum = sum + value;
    if (count > nautilus::val<double>{0.0})
    {
        const auto squaredDeviations = readState(aggregationState, offsetSxx);
        const auto deviation = value * newCount - newSum;
        writeState(aggregationState, offsetSxx, squaredDeviations + deviation * deviation / (newCount * count));
    }
    else
    {
        writeState(aggregationState, offsetSxx, value - value);
    }
    writeState(aggregationState, offsetN, newCount);
    writeState(aggregationState, offsetSx, newSum);
}
}

template <VarianceKind Kind>
VarianceAggregationPhysicalFunction<Kind>::VarianceAggregationPhysicalFunction(
    DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), std::move(inputFunction), std::move(resultFieldIdentifier))
{
}

template <VarianceKind Kind>
void VarianceAggregationPhysicalFunction<Kind>::lift(
    const nautilus::val<AggregationState*>& aggregationState,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider& pipelineMemoryProvider,
    const Record& record)
{
    const auto value = inputFunction.execute(record, pipelineMemoryProvider.arena);
    const auto number = asDouble(value);
    if (inputType.nullable)
    {
        /// SQL-standard: a NULL value is skipped
        if (not value.isNull())
        {
            accumulate(aggregationState, number);
        }
    }
    else
    {
        accumulate(aggregationState, number);
    }
}

template <VarianceKind Kind>
void VarianceAggregationPhysicalFunction<Kind>::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    BorrowedNautilusBuffer,
    const nautilus::val<AggregationState*> aggregationState2,
    BorrowedNautilusBuffer,
    PipelineMemoryProvider&)
{
    /// float8_combine: Sxx = Sxx1 + Sxx2 + N1 * N2 * (Sx1 / N1 - Sx2 / N2)^2 / N, an empty state leaving the other as it is
    const auto count1 = readState(aggregationState1, offsetN);
    const auto count2 = readState(aggregationState2, offsetN);
    const auto sum2 = readState(aggregationState2, offsetSx);
    const auto squaredDeviations2 = readState(aggregationState2, offsetSxx);
    if (count1 == nautilus::val<double>{0.0})
    {
        writeState(aggregationState1, offsetN, count2);
        writeState(aggregationState1, offsetSx, sum2);
        writeState(aggregationState1, offsetSxx, squaredDeviations2);
    }
    else if (count2 > nautilus::val<double>{0.0})
    {
        const auto sum1 = readState(aggregationState1, offsetSx);
        const auto squaredDeviations1 = readState(aggregationState1, offsetSxx);
        const auto count = count1 + count2;
        const auto meanDifference = sum1 / count1 - sum2 / count2;
        writeState(aggregationState1, offsetN, count);
        writeState(aggregationState1, offsetSx, sum1 + sum2);
        writeState(
            aggregationState1,
            offsetSxx,
            squaredDeviations1 + squaredDeviations2 + count1 * count2 * meanDifference * meanDifference / count);
    }
}

template <VarianceKind Kind>
Record VarianceAggregationPhysicalFunction<Kind>::lower(
    const nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider&)
{
    /// float8_var_pop divides Sxx by N and is NULL for N = 0; float8_var_samp divides by N - 1 and is NULL for N <= 1
    const auto count = readState(aggregationState, offsetN);
    const auto squaredDeviations = readState(aggregationState, offsetSxx);
    const auto divisor = Kind == VarianceKind::Population ? count : count - nautilus::val<double>{1.0};
    if (divisor <= nautilus::val<double>{0.0})
    {
        const VarVal nullResult{nautilus::val<double>{0.0}, true, nautilus::val<bool>{true}};
        return Record({{resultFieldIdentifier, nullResult}});
    }
    const VarVal variance{squaredDeviations / divisor, true, nautilus::val<bool>{false}};
    return Record({{resultFieldIdentifier, variance}});
}

template <VarianceKind Kind>
void VarianceAggregationPhysicalFunction<Kind>::reset(
    const nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider&)
{
    const auto memArea = static_cast<nautilus::val<int8_t*>>(aggregationState);
    nautilus::memset(memArea, 0, getSizeOfStateInBytes());
}

template <VarianceKind Kind>
void VarianceAggregationPhysicalFunction<Kind>::cleanup(nautilus::val<AggregationState*>)
{
}

template <VarianceKind Kind>
size_t VarianceAggregationPhysicalFunction<Kind>::getSizeOfStateInBytes() const
{
    return 3 * sizeof(double);
}

template <VarianceKind Kind>
AggregationPhysicalFunctionRegistryReturnType
VarianceAggregationPhysicalFunction<Kind>::create(AggregationPhysicalFunctionRegistryArguments arguments)
{
    return std::make_shared<VarianceAggregationPhysicalFunction<Kind>>(
        std::move(arguments.inputType), std::move(arguments.resultType), arguments.inputFunction, arguments.resultFieldIdentifier);
}

template class VarianceAggregationPhysicalFunction<VarianceKind::Population>;
template class VarianceAggregationPhysicalFunction<VarianceKind::Sample>;

}
