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
#include <memory>
#include <span>
#include <string>
#include <vector>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <AggregationPhysicalFunctionRegistry.hpp>

namespace NES
{

/// The C++ functions running one overload of a MEOS aggregate over its state, generated from the MEOS catalog
/// by tools/codegen/codegen_nebula_sql.py as the entry points of the MeosAggregate template of
/// MeosAggregateRuntime.hpp. Each takes the slot of the state; the argument is handed over as a MEOS kernel
/// takes it, and the result answered as a MEOS kernel answers it, with its null flag. The constants of the
/// query following the argument are handed over as one block (MeosRuntime::literal).
struct MeosAggregateKernels
{
    /// The empty state laid in the slot
    void (*initialize)(int8_t* slot);
    /// The argument of one row gathered in children of the parent buffer, allocated by the provider; whether the
    /// argument is variable-sized
    void (*update)(int8_t* slot, TupleBuffer* parent, AbstractBufferProvider* provider, int8_t* argument, bool varsized);
    /// The rows of the source slot copied into the target's, the source left whole
    void (*combine)(int8_t* target, TupleBuffer* targetParent, int8_t* source, TupleBuffer* sourceParent, AbstractBufferProvider* provider);
    /// The answer of the result over the rows the slot gathers and the constants, which are left whole
    int8_t* (*finish)(int8_t* slot, TupleBuffer* parent, int8_t* literals, bool varsized, int8_t* out);
    /// The size of the state
    size_t stateSize;
};

/// A MEOS aggregate over one overload: it gathers the value of its argument in buffers of the hash map, joins the
/// states of slices and threads by copying their values, leaving a slice whole for every window it lies in, and
/// answers the final's result over the values folded by the transition, null over no value. A variable-sized argument is passed as a MeosPhysicalFunction passes it; a number is cast to the type the
/// overload states. A null argument is skipped, the aggregate being STRICT.
class MeosAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    MeosAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction inputFunction,
        Record::RecordFieldIdentifier resultFieldIdentifier,
        DataType::Type argumentType,
        std::span<const DataType::Type> literalTypes,
        const std::vector<LogicalFunction>& literals,
        const MeosAggregateKernels& kernels);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        BorrowedNautilusBuffer parentBuffer1,
        nautilus::val<AggregationState*> aggregationState2,
        BorrowedNautilusBuffer parentBuffer2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(
        nautilus::val<AggregationState*> aggregationState,
        BorrowedNautilusBuffer parentBuffer,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~MeosAggregationPhysicalFunction() override = default;

    /// The constants of the query, each in the form a kernel takes an argument of its type: the bytes a
    /// variable-sized one names, kept alive with the block
    struct LiteralBlock
    {
        std::vector<std::vector<int8_t>> values;
        std::vector<int8_t> block;
    };

private:
    DataType::Type argumentType;
    /// The block of the constants, shared by the copies of the function the compiled query points into
    std::shared_ptr<LiteralBlock> literals;
    MeosAggregateKernels kernels;
};

/// The physical aggregation of one overload of a MEOS aggregate, the constants of the query read from the
/// MeosAggregationLogicalFunction lowered, as the registry entries tools/codegen/codegen_nebula_sql.py
/// generates build it
std::shared_ptr<AggregationPhysicalFunction> meosAggregation(
    AggregationPhysicalFunctionRegistryArguments arguments,
    DataType::Type argumentType,
    std::span<const DataType::Type> literalTypes,
    const MeosAggregateKernels& kernels);

}
