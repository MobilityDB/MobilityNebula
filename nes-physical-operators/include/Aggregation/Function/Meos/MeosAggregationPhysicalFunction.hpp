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
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// The C++ functions running one overload of a MEOS aggregate over its state, generated from the MEOS
/// catalog by tools/codegen/codegen_nebula_sql.py as the entry points of the MeosAggregate template of
/// nes-plugins/MEOS/include/MeosAggregateRuntime.hpp. Each takes the slot of the state; the argument is
/// handed over as a MEOS kernel takes it, and the result answered as a MEOS kernel answers it.
struct MeosAggregateKernels
{
    /// The empty state laid in the slot
    void (*initialize)(int8_t* slot);
    /// The argument of one row folded into the state
    void (*update)(int8_t* slot, int8_t* argument);
    /// The state of the source slot joined into the target's, the source left empty
    void (*combine)(int8_t* target, int8_t* source);
    /// The answer of the result over the state, which is left empty
    int8_t* (*finish)(int8_t* slot, int8_t* out);
    /// What the state holds released
    void (*destroy)(int8_t* slot);
    /// The size of the state
    size_t stateSize;
};

/// A MEOS aggregate over one overload: it folds the value of its argument into a MEOS state, joins the
/// states of slices and threads with the aggregate's combine, and answers the final's result. A
/// variable-sized argument is passed as it is stored; a number is cast to the type the overload states.
class MeosAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    MeosAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        DataType::Type argumentType,
        const MeosAggregateKernels& kernels);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~MeosAggregationPhysicalFunction() override = default;

private:
    DataType::Type argumentType;
    MeosAggregateKernels kernels;
};

}
