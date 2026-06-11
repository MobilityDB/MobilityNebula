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

#include <Aggregation/Function/Meos/TnumberExtentAggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <ctime>
#include <mutex>
#include <cstring>
#include <cstdio>
#include <string>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/PagedVector/PagedVector.hpp>
#include <Nautilus/Interface/PagedVector/PagedVectorRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

#include <MEOSWrapper.hpp>
extern "C" {
#include <meos.h>
}

namespace NES
{

constexpr static std::string_view ValueFieldName = "value";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex meos_tnumberextent_mutex;


TnumberExtentAggregationPhysicalFunction::TnumberExtentAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction valueFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), valueFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , valueFunction(std::move(valueFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void TnumberExtentAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({
        {std::string(ValueFieldName), valueValue},
        {std::string(TimestampFieldName), timestampValue}
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void TnumberExtentAggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        { vector1->copyFrom(*vector2); },
        memArea1,
        memArea2);
}

Nautilus::Record TnumberExtentAggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {
            return pagedVector->getTotalNumberOfEntries();
        },
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {
        auto emptyVarSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVarSized);
        return resultRecord;
    }

    auto sequenceStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 80 + 50;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "{");
            return buffer;
        },
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;

        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        const auto timestampRaw = itemRecord.read(std::string(TimestampFieldName));

        auto value = valueRaw.cast<nautilus::val<double>>();
        auto timestamp = timestampRaw.cast<nautilus::val<int64_t>>();

        sequenceStr = nautilus::invoke(
            +[](char* buffer, double valueVal, int64_t tsVal, int64_t counter) -> char*
            {
                if (counter > 0) {
                    strcat(buffer, ", ");
                }

                long long adjustedTime;
                if (tsVal > 1000000000000LL) {
                    adjustedTime = tsVal / 1000;
                } else {
                    adjustedTime = tsVal;
                }

                std::string timestampString = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                const char* timestampStr = timestampString.c_str();

                char itemStr[80];
                sprintf(itemStr, "%.6f@%s", valueVal, timestampStr);
                strcat(buffer, itemStr);
                return buffer;
            },
            sequenceStr,
            value,
            timestamp,
            pointCounter);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }

    sequenceStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {
            strcat(buffer, "}");
            return buffer;
        },
        sequenceStr);

    auto boxStr = nautilus::invoke(
        +[](const char* seqStr) -> char*
        {
            if (!seqStr || strlen(seqStr) == 0) {
                free((void*)seqStr);
                return (char*)nullptr;
            }

            std::lock_guard<std::mutex> lock(meos_tnumberextent_mutex);

            Temporal* temp = tfloat_in(seqStr);
            free((void*)seqStr);
            if (!temp) {
                return (char*)nullptr;
            }

            TBox* aggBox = tnumber_extent_transfn(nullptr, temp);
            free(temp);
            if (!aggBox) {
                return (char*)nullptr;
            }

            char* boxText = tbox_out(aggBox, 15);
            free(aggBox);
            return boxText;
        },
        sequenceStr);

    const auto boxStrLen = nautilus::invoke(
        +[](const char* s) -> size_t { return s ? strlen(s) : (size_t) 0; },
        boxStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {
            if (s) {
                memcpy(dest, s, len);
                free((void*)s);
            }
        },
        variableSized.getContent(),
        boxStr,
        boxStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}

void TnumberExtentAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TnumberExtentAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void TnumberExtentAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTnumberExtentAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TNUMBER_EXTENT aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}

} // namespace NES
