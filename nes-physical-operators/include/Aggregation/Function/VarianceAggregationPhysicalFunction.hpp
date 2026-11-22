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
#include <memory>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/NautilusBuffer.hpp>
#include <Interface/Record.hpp>
#include <Operators/Windows/Aggregations/VarianceAggregationLogicalFunction.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

namespace NES
{

/// VAR_POP and VAR_SAMP as PostgreSQL computes them over float8 (float8_accum, float8_combine, float8_var_pop and
/// float8_var_samp): the state is the number of values N, their sum Sx and the sum Sxx of their squared deviations from
/// the mean. Each value updates Sxx by the algorithm of Youngs and Cramer, and two states merge by the formula of Chan,
/// Golub and LeVeque, so neither subtracts two large sums. NULL values are skipped. A NaN or an infinite value makes the
/// result NaN.
template <VarianceKind Kind>
class VarianceAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    VarianceAggregationPhysicalFunction(
        DataType inputType, DataType resultType, PhysicalFunction inputFunction, Record::RecordFieldIdentifier resultFieldIdentifier);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        BorrowedNautilusBuffer,
        nautilus::val<AggregationState*> aggregationState2,
        BorrowedNautilusBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState, BorrowedNautilusBuffer, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~VarianceAggregationPhysicalFunction() override = default;

    static AggregationPhysicalFunctionRegistryReturnType create(AggregationPhysicalFunctionRegistryArguments arguments);
};

using VarPopAggregationPhysicalFunction = VarianceAggregationPhysicalFunction<VarianceKind::Population>;
using VarSampAggregationPhysicalFunction = VarianceAggregationPhysicalFunction<VarianceKind::Sample>;

extern template class VarianceAggregationPhysicalFunction<VarianceKind::Population>;
extern template class VarianceAggregationPhysicalFunction<VarianceKind::Sample>;

}
