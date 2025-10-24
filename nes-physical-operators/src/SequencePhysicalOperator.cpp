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

#include <SequencePhysicalOperator.hpp>

#include <cstdint>
#include <utility>
#include <ErrorHandling.hpp>
#include <Nautilus/Interface/RecordBuffer.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Runtime/QueryTerminationType.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <ExecutionContext.hpp>
#include <PipelineExecutionContext.hpp>
#include <SequenceOperatorHandler.hpp>
#include <function.hpp>

namespace NES::Runtime::Execution::Operators
{

namespace
{
[[nodiscard]] SequenceOperatorHandler* toSequenceHandler(OperatorHandler* handler)
{
    PRECONDITION(handler != nullptr, "SequencePhysicalOperator: operator handler must not be null");
    auto* sequenceHandler = dynamic_cast<SequenceOperatorHandler*>(handler);
    PRECONDITION(sequenceHandler != nullptr, "SequencePhysicalOperator: operator handler is not a SequenceOperatorHandler");
    return sequenceHandler;
}
}

void SequencePhysicalOperator::open(ExecutionContext& executionCtx, Nautilus::RecordBuffer& recordBuffer) const
{
    auto buffer = nautilus::invoke(
        +[](OperatorHandler* handler, uint8_t* bufferMemory) -> Memory::TupleBuffer*
        {
            auto* sequenceHandler = toSequenceHandler(handler);
            return sequenceHandler->getNextBuffer(bufferMemory).value_or(nullptr);
        },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        recordBuffer.getBuffer());

    while (buffer)
    {
        RecordBuffer nextBufferInSequence{buffer};

        scan.open(executionCtx, nextBufferInSequence);
        scan.close(executionCtx, nextBufferInSequence);

        buffer = nautilus::invoke(
            +[](OperatorHandler* handler, Memory::TupleBuffer* tupleBuffer) -> Memory::TupleBuffer*
            {
                auto* sequenceHandler = toSequenceHandler(handler);
                return sequenceHandler->markBufferAsDone(tupleBuffer).value_or(nullptr);
            },
            executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
            buffer);
    }
}
void SequencePhysicalOperator::setup(ExecutionContext& executionCtx) const
{
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* ctx)
        { toSequenceHandler(handler)->start(*ctx, 0); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        executionCtx.pipelineContext);
    scan.setup(executionCtx);
}
void SequencePhysicalOperator::terminate(ExecutionContext& executionCtx) const
{
    scan.terminate(executionCtx);
    nautilus::invoke(
        +[](OperatorHandler* handler, PipelineExecutionContext* ctx)
        { toSequenceHandler(handler)->stop(QueryTerminationType::Graceful, *ctx); },
        executionCtx.getGlobalOperatorHandler(operatorHandlerIndex),
        executionCtx.pipelineContext);
}
void SequencePhysicalOperator::setChild(PhysicalOperator child)
{
    scan.setChild(child);
}
std::optional<struct PhysicalOperator> SequencePhysicalOperator::getChild() const
{
    return scan.getChild();
}
}
