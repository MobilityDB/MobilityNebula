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
#include <Nautilus/Interface/Record.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Nautilus/Interface/TimestampRef.hpp>
#include <Time/Timestamp.hpp>
#include <Util/Common.hpp>
#include <Watermark/TimeFunction.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <OperatorState.hpp>
#include <PhysicalOperator.hpp>

namespace NES
{

struct WatermarkState final : OperatorState
{
    explicit WatermarkState() = default;
    nautilus::val<Timestamp> currentWatermark{Timestamp(Timestamp::INITIAL_VALUE)};
};

EventTimeWatermarkAssignerPhysicalOperator::EventTimeWatermarkAssignerPhysicalOperator(
    EventTimeFunction timeFunction, const uint64_t allowedLateness)
    : timeFunction(std::move(std::move(timeFunction))), allowedLateness(allowedLateness) { };

void EventTimeWatermarkAssignerPhysicalOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    openChild(executionCtx, recordBuffer);
    executionCtx.setLocalOperatorState(id, std::make_unique<WatermarkState>());
    timeFunction.open(executionCtx, recordBuffer);
}

void EventTimeWatermarkAssignerPhysicalOperator::execute(ExecutionContext& ctx, Record& record) const
{
    auto* const state = dynamic_cast<WatermarkState*>(ctx.getLocalState(id));
    const auto tsField = timeFunction.getTs(ctx, record);
    if (tsField > state->currentWatermark)
    {
        state->currentWatermark = tsField;
    }
    /// call next operator
    executeChild(ctx, record);
}

void EventTimeWatermarkAssignerPhysicalOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    PRECONDITION(
        NES::Util::instanceOf<const WatermarkState>(*executionCtx.getLocalState(id)), /// TODO #1035
        "Expects the local state to be of type WatermarkState");
    auto* const state = dynamic_cast<WatermarkState*>(executionCtx.getLocalState(id));
    /// The largest event time read less the lateness the query allows, none before the first instant
    const nautilus::val<uint64_t> largest = state->currentWatermark.convertToValue();
    const nautilus::val<uint64_t> lateness = allowedLateness;
    nautilus::val<uint64_t> watermark = Timestamp::INITIAL_VALUE;
    if (largest > lateness)
    {
        watermark = largest - lateness;
    }
    executionCtx.watermarkTs = nautilus::val<Timestamp>(watermark);
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
