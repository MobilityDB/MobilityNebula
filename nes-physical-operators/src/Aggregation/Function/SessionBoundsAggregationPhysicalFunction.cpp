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

#include <Aggregation/Function/SessionBoundsAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <limits>
#include <utility>
#include <Aggregation/Function/AggregationPhysicalFunction.hpp>
#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Nautilus/DataTypes/DataTypesUtil.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <ExecutionContext.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{
/// The state holds the first event time, then the last one
constexpr size_t LAST_OFFSET = sizeof(uint64_t);

nautilus::val<uint64_t*> firstRef(const nautilus::val<AggregationState*>& aggregationState)
{
    return static_cast<nautilus::val<uint64_t*>>(aggregationState);
}

nautilus::val<uint64_t*> lastRef(const nautilus::val<AggregationState*>& aggregationState)
{
    return static_cast<nautilus::val<uint64_t*>>(
        static_cast<nautilus::val<int8_t*>>(aggregationState) + nautilus::val<uint64_t>(LAST_OFFSET));
}
}

SessionBoundsAggregationPhysicalFunction::SessionBoundsAggregationPhysicalFunction(
    PhysicalFunction timestampFunction, const uint64_t millisecondsMultiplier)
    : AggregationPhysicalFunction(
          DataTypeProvider::provideDataType(DataType::Type::UINT64),
          DataTypeProvider::provideDataType(DataType::Type::UINT64),
          std::move(timestampFunction),
          "$session_bounds")
    , millisecondsMultiplier(millisecondsMultiplier)
{
}

nautilus::val<uint64_t> SessionBoundsAggregationPhysicalFunction::readFirst(const nautilus::val<AggregationState*>& aggregationState)
{
    return Nautilus::Util::readValueFromMemRef<uint64_t>(static_cast<nautilus::val<int8_t*>>(firstRef(aggregationState)));
}

nautilus::val<uint64_t> SessionBoundsAggregationPhysicalFunction::readLast(const nautilus::val<AggregationState*>& aggregationState)
{
    return Nautilus::Util::readValueFromMemRef<uint64_t>(static_cast<nautilus::val<int8_t*>>(lastRef(aggregationState)));
}

void SessionBoundsAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Record& record)
{
    /// The event time in milliseconds, as the time function of the window computes it
    const nautilus::val<uint64_t> ts = inputFunction.execute(record, pipelineMemoryProvider.arena).cast<nautilus::val<uint64_t>>()
        * nautilus::val<uint64_t>(millisecondsMultiplier);
    if (ts < readFirst(aggregationState))
    {
        *firstRef(aggregationState) = ts;
    }
    if (ts > readLast(aggregationState))
    {
        *lastRef(aggregationState) = ts;
    }
}

void SessionBoundsAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    const nautilus::val<uint64_t> first2 = readFirst(aggregationState2);
    const nautilus::val<uint64_t> last2 = readLast(aggregationState2);
    if (first2 < readFirst(aggregationState1))
    {
        *firstRef(aggregationState1) = first2;
    }
    if (last2 > readLast(aggregationState1))
    {
        *lastRef(aggregationState1) = last2;
    }
}

Record SessionBoundsAggregationPhysicalFunction::lower(const nautilus::val<AggregationState*>, PipelineMemoryProvider&)
{
    return Record{};
}

void SessionBoundsAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    *firstRef(aggregationState) = nautilus::val<uint64_t>(std::numeric_limits<uint64_t>::max());
    *lastRef(aggregationState) = nautilus::val<uint64_t>(0);
}

void SessionBoundsAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*>)
{
}

size_t SessionBoundsAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return 2 * sizeof(uint64_t);
}

}
