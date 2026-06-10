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

#include <Aggregation/Function/Meos/Rgeo/TrgeometryInstantsAggregationPhysicalFunction.hpp>

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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

#include <MEOSWrapper.hpp>
extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_pose.h>
#include <meos_rgeo.h>
}

namespace NES
{

constexpr static std::string_view XFieldName = "x";
constexpr static std::string_view YFieldName = "y";
constexpr static std::string_view ThetaFieldName = "theta";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex meos_trgeometryinstants_mutex;


TrgeometryInstantsAggregationPhysicalFunction::TrgeometryInstantsAggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction xFunctionParam,
    PhysicalFunction yFunctionParam,
    PhysicalFunction thetaFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), xFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , xFunction(std::move(xFunctionParam))
    , yFunction(std::move(yFunctionParam))
    , thetaFunction(std::move(thetaFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{
}

void TrgeometryInstantsAggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto xValue = xFunction.execute(record, pipelineMemoryProvider.arena);
    auto yValue = yFunction.execute(record, pipelineMemoryProvider.arena);
    auto thetaValue = thetaFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({
        {std::string(XFieldName), xValue},
        {std::string(YFieldName), yValue},
        {std::string(ThetaFieldName), thetaValue},
        {std::string(TimestampFieldName), timestampValue}
    });

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}

void TrgeometryInstantsAggregationPhysicalFunction::combine(
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

Nautilus::Record TrgeometryInstantsAggregationPhysicalFunction::lower(
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

    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 200 + 50;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "[");
            return buffer;
        },
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {
        const auto itemRecord = *candidateIt;

        const auto xValue = itemRecord.read(std::string(XFieldName));
        const auto yValue = itemRecord.read(std::string(YFieldName));
        const auto thetaValue = itemRecord.read(std::string(ThetaFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));

        auto x = xValue.cast<nautilus::val<double>>();
        auto y = yValue.cast<nautilus::val<double>>();
        auto theta = thetaValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

        trajectoryStr = nautilus::invoke(
            +[](char* buffer, double xVal, double yVal, double thetaVal, int64_t tsVal, int64_t counter) -> char*
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

                char poseStr[160];
                sprintf(poseStr, "Pose(Point(%.6f %.6f),%.6f)@%s", xVal, yVal, thetaVal, timestampStr);
                strcat(buffer, poseStr);
                return buffer;
            },
            trajectoryStr,
            x,
            y,
            theta,
            timestamp,
            pointCounter);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }

    trajectoryStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {
            strcat(buffer, "]");
            return buffer;
        },
        trajectoryStr);

    auto boxStr = nautilus::invoke(
        +[](const char* trajStr) -> char*
        {
            if (!trajStr || strlen(trajStr) == 0) {
                free((void*)trajStr);
                return (char*)nullptr;
            }

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock(meos_trgeometryinstants_mutex);

            std::string trajString(trajStr);
            Temporal* _poseSeq = tpose_in(trajString.c_str());
            free((void*)trajStr);
            if (!_poseSeq) {
                return (char*)nullptr;
            }
            GSERIALIZED* _refGeom = geom_in("Polygon((0 0,1 0,1 1,0 1,0 0))", -1);
            if (!_refGeom) {
                free(_poseSeq);
                return (char*)nullptr;
            }
            void* temp = geo_tpose_to_trgeometry(_refGeom, _poseSeq);
            free(_refGeom);
            free(_poseSeq);
            if (!temp) {
                return (char*)nullptr;
            }

            int _cnt = 0;
            void** arr = (void**) trgeometry_instants(static_cast<Temporal*>(temp), &_cnt);
            MEOS::Meos::freeTemporalObject(temp);
            if (!arr || _cnt <= 0) {
                if (arr) free(arr);
                return (char*)nullptr;
            }

            std::string _s = "{";
            for (int _i = 0; _i < _cnt; _i++) {
                if (_i) _s += ", ";
                size_t _z = 0;
                char* _e = temporal_as_hexwkb((const Temporal*) arr[_i], 0x04 /* WKB_EXTENDED */, &_z);
                if (_e) { _s += _e; free(_e); }
                free(arr[_i]);
            }
            _s += "}";
            free(arr);
            return strdup(_s.c_str());
        },
        trajectoryStr);

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

void TrgeometryInstantsAggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        },
        aggregationState);
}

size_t TrgeometryInstantsAggregationPhysicalFunction::getSizeOfStateInBytes() const
{
    return sizeof(Nautilus::Interface::PagedVector);
}

void TrgeometryInstantsAggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        },
        aggregationState);
}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::RegisterTrgeometryInstantsAggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{
    throw std::runtime_error("TrgeometryInstants aggregation cannot be created through the registry. "
                             "It requires four field functions (x, y, theta, timestamp)");
}

} // namespace NES
