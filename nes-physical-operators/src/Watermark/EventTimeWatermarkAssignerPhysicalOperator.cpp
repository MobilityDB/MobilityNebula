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
#include <Watermark/EventTimeWatermarkAssignerPhysicalOperator.hpp>

#include <memory>
#include <optional>
#include <utility>
#include <Interface/Record.hpp>
#include <Interface/RecordBuffer.hpp>
#include <Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ExecutionContext.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

EventTimeWatermarkAssignerPhysicalOperator::EventTimeWatermarkAssignerPhysicalOperator(
    EventTimeFunction timeFunction, const uint64_t allowedLateness)
    : timeFunction(std::move(timeFunction)), allowedLateness(allowedLateness) { };

void EventTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    /// Buffers from sources carry no watermark; the assigner is where event-time progress starts.
    if (executionCtx.watermarkTs == nautilus::val<Timestamp>(Timestamp(Timestamp::INVALID_VALUE)))
    {
        executionCtx.watermarkTs = nautilus::val<Timestamp>(Timestamp(Timestamp::INITIAL_VALUE));
    }
    openChild(executionCtx, recordBuffer);
    timeFunction.open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    /// The watermark is the largest event time read less the lateness the query allows, none before the first instant
    const nautilus::val<uint64_t> ts = timeFunction.getTs(ctx, record).convertToValue();
    nautilus::val<uint64_t> trailing = Timestamp::INITIAL_VALUE;
    if (ts > nautilus::val<uint64_t>(allowedLateness))
    {
        trailing = ts - nautilus::val<uint64_t>(allowedLateness);
    }
    const nautilus::val<Timestamp> candidate{trailing};
    if (candidate > ctx.watermarkTs)
    {
        ctx.watermarkTs = candidate;
    }
    /// call next operator
    executeChild(ctx, record);
}

void EventTimeWatermarkAssignerPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PhysicalOperatorConcept::close(executionCtx, recordBuffer);
}

std::optional<PhysicalOperator> EventTimeWatermarkAssignerPhysicalOperator::getChild() const
{
    return child;
}

void EventTimeWatermarkAssignerPhysicalOperator::setChild(PhysicalOperator child)
{
    this->child = std::move(child);
}

}
