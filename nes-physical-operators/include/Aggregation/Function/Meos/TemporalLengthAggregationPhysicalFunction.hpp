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
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <val_concepts.hpp>

namespace NES
{

/**
 * @brief Aggregation function that returns the spheroidal length in metres of
 * the per-(window, group) trajectory built from the (lon, lat, timestamp)
 * tuples lifted into the aggregation state.
 *
 * Same lift / combine / reset shape as TemporalSequenceAggregationPhysicalFunction;
 * the lower step orders the events by time, builds the linear tgeogpoint sequence
 * (SRID 4326) through them and returns MEOS' tpoint_length(seq, true), the
 * spheroidal length in metres, as a single FLOAT64. A window holding one instant
 * has length 0; a window holding two different positions at one timestamp states
 * no trajectory and yields NaN.
 *
 * Used by BerlinMOD-Q6 ("cumulative distance per vehicle").
 */
class TemporalLengthAggregationPhysicalFunction : public AggregationPhysicalFunction
{
public:
    TemporalLengthAggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction lonFunctionParam,
        PhysicalFunction latFunctionParam,
        PhysicalFunction timestampFunctionParam,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef);
    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Nautilus::Record& record)
        override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~TemporalLengthAggregationPhysicalFunction() override = default;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    PhysicalFunction lonFunction;
    PhysicalFunction latFunction;
    PhysicalFunction timestampFunction;
};

}
