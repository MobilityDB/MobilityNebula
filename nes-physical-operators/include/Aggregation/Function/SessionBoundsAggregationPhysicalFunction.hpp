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
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <val_concepts.hpp>

namespace NES
{

/// The first and the last event time, in milliseconds, of the records of a key.
/// A session window appends it after the aggregations of the query: it tells where a key's session starts,
/// and whether the key's records of the next slice follow its last record by less than the gap.
/// It writes no field of the output record.
class SessionBoundsAggregationPhysicalFunction final : public AggregationPhysicalFunction
{
public:
    SessionBoundsAggregationPhysicalFunction(PhysicalFunction timestampFunction, uint64_t millisecondsMultiplier);

    void lift(
        const nautilus::val<AggregationState*>& aggregationState,
        PipelineMemoryProvider& pipelineMemoryProvider,
        const Record& record) override;
    void combine(
        nautilus::val<AggregationState*> aggregationState1,
        nautilus::val<AggregationState*> aggregationState2,
        PipelineMemoryProvider& pipelineMemoryProvider) override;
    Record lower(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;

    static nautilus::val<uint64_t> readFirst(const nautilus::val<AggregationState*>& aggregationState);
    static nautilus::val<uint64_t> readLast(const nautilus::val<AggregationState*>& aggregationState);

private:
    uint64_t millisecondsMultiplier;
};

}
