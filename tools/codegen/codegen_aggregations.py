#!/usr/bin/env python3
"""MobilityNebula MEOS-aggregation generator.

Companion to ``codegen_nebula.py`` (per-event ops). This generator targets
the WINDOWED-aggregation surface: MEOS scalar functions of the shape
``<scalar> fn(const Temporal*)`` where the Temporal* is a per-(window,
group) sequence assembled across multiple events.

For each operator in the JSON descriptor list, emits four C++ files
mirroring mariana's hand-written TemporalLengthAggregation 1:1:

  * nes-logical-operators/include/Operators/Windows/Aggregations/Meos/
        XXXAggregationLogicalFunction.hpp
  * nes-logical-operators/src/Operators/Windows/Aggregations/Meos/
        XXXAggregationLogicalFunction.cpp
  * nes-physical-operators/include/Aggregation/Function/Meos/
        XXXAggregationPhysicalFunction.hpp
  * nes-physical-operators/src/Aggregation/Function/Meos/
        XXXAggregationPhysicalFunction.cpp

And idempotently injects into 5 in-tree shared files:

  * nes-sql-parser/AntlrSQL.g4
        - lexer-token entries
        - functionName: alternation list
  * nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp
        - case AntlrSQLLexer::TOKEN: dispatch in the dedicated-token switch
        - else if (funcName == "TOKEN") dispatch in the IDENTIFIER fallback chain
  * nes-query-optimizer/src/RewriteRules/LowerToPhysical/
        LowerToPhysicalWindowedAggregation.cpp
        - if (name == "Xxx") { ... } block lowering logical → physical
  * nes-{logical,physical}-operators/.../{Aggregation*}/CMakeLists.txt
        - add_plugin(...) per layer

All injections are bracketed with
``/* BEGIN CODEGEN AGGREGATION GLUE: TOKEN */ ... /* END ... */`` markers
so re-runs are no-ops and pre-existing hand-written cases (mariana's) are
detected by raw token match and skipped.

Two lift-shape branches, picked by descriptor ``input_shape``:
  * ``tgeo``   — 3 fields per event (lon, lat, ts); lower builds
                 ``{Point(lon lat)@ts, ...}`` trajectory string parsed via
                 ``MEOS::Meos::parseTemporalPoint``.
  * ``tnumber``— 2 fields per event (value, ts); lower builds
                 ``{value@ts, ...}`` string parsed via ``tfloat_in`` or
                 ``tint_in`` per descriptor.

Usage:
    python3 codegen_aggregations.py --input <descriptor.json> \\
                                    --output-root /path/to/MobilityNebula \\
                                    [--no-parser-glue] [--no-cmake-entries] [--no-optimizer-glue]
"""
import argparse
import json
import re
import sys
from pathlib import Path

# ===========================================================================
# Logical-layer .hpp template (mirrors TemporalLengthAggregationLogicalFunction.hpp).
# ===========================================================================
LOGICAL_HPP_TGEO = """\
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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{{

/**
 * @brief {comment_one_liner}
 *
 * Three-input (lon, lat, ts) tgeo aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) trajectory
 * and calls MEOS `{meos_scalar_fn}` to fold it to a single scalar.
 */
class {nebula_name}AggregationLogicalFunction : public WindowAggregationLogicalFunction
{{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~{nebula_name}AggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const {{ return true; }}

    [[nodiscard]] const FieldAccessLogicalFunction& getLonField() const noexcept {{ return lonField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getLatField() const noexcept {{ return latField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept {{ return timestampField; }}

private:
    static constexpr std::string_view NAME = "{class_name_token}";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::{final_stamp_type};

    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
}};
}}
"""

LOGICAL_HPP_TNUMBER = """\
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

#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>

namespace NES
{{

/**
 * @brief {comment_one_liner}
 *
 * Two-input (value, ts) tnumber aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) tnumber
 * sequence and calls MEOS `{meos_scalar_fn}` to fold it to a single scalar.
 */
class {nebula_name}AggregationLogicalFunction : public WindowAggregationLogicalFunction
{{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& valueField, const FieldAccessLogicalFunction& timestampField);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& valueField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~{nebula_name}AggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const {{ return true; }}

    [[nodiscard]] const FieldAccessLogicalFunction& getValueField() const noexcept {{ return valueField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept {{ return timestampField; }}

private:
    static constexpr std::string_view NAME = "{class_name_token}";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::{final_stamp_type};

    FieldAccessLogicalFunction valueField;
    FieldAccessLogicalFunction timestampField;
}};
}}
"""

# Logical .cpp templates — share scaffold (ctor, inferStamp, serialize, registry)
# but differ in field count (3 for tgeo, 2 for tnumber).
LOGICAL_CPP_TGEO = """\
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

#include <Operators/Windows/Aggregations/Meos/{nebula_name}AggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/TemporalAggregationSerde.hpp>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{{

{nebula_name}AggregationLogicalFunction::{nebula_name}AggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          lonField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField,
          asField)
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(lonField, latField, timestampField, lonField);
}}

std::string_view {nebula_name}AggregationLogicalFunction::getName() const noexcept
{{
    return NAME;
}}

void {nebula_name}AggregationLogicalFunction::inferStamp(const Schema& schema)
{{
    lonField = lonField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    latField = latField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = lonField;

    if (!lonField.getDataType().isNumeric() || !latField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {{
        throw CannotInferSchema("{nebula_name}AggregationLogicalFunction: lon, lat, and timestamp fields must be numeric.");
    }}

    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {{
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }}
    else
    {{
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }}
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
    inputStamp = onField.getDataType();
}}

NES::SerializableAggregationFunction {nebula_name}AggregationLogicalFunction::serialize() const
{{
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(lonField, latField, timestampField, asField);
    saf.set_type(std::string(NAME));
    return saf;
}}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{{
    if (arguments.fields.size() == 4)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {{}}",
        arguments.fields.size());
}}

}} // namespace NES
"""

LOGICAL_CPP_TNUMBER = """\
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

#include <Operators/Windows/Aggregations/Meos/{nebula_name}AggregationLogicalFunction.hpp>

#include <memory>
#include <string>
#include <string_view>
#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/Schema.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>
#include <Serialization/TemporalAggregationSerde.hpp>

#include <AggregationLogicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES
{{

{nebula_name}AggregationLogicalFunction::{nebula_name}AggregationLogicalFunction(
    const FieldAccessLogicalFunction& valueField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          valueField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          valueField,
          asField)
    , valueField(valueField)
    , timestampField(timestampField)
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& valueField,
    const FieldAccessLogicalFunction& timestampField)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(valueField, timestampField, valueField);
}}

std::string_view {nebula_name}AggregationLogicalFunction::getName() const noexcept
{{
    return NAME;
}}

void {nebula_name}AggregationLogicalFunction::inferStamp(const Schema& schema)
{{
    valueField = valueField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = valueField;

    if (!valueField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {{
        throw CannotInferSchema("{nebula_name}AggregationLogicalFunction: value and timestamp fields must be numeric.");
    }}

    const auto onFieldName = onField.getFieldName();
    const auto asFieldName = asField.getFieldName();
    const auto attributeNameResolver = onFieldName.substr(0, onFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
    if (asFieldName.find(Schema::ATTRIBUTE_NAME_SEPARATOR) == std::string::npos)
    {{
        asField = asField.withFieldName(attributeNameResolver + asFieldName).get<FieldAccessLogicalFunction>();
    }}
    else
    {{
        const auto fieldName = asFieldName.substr(asFieldName.find_last_of(Schema::ATTRIBUTE_NAME_SEPARATOR) + 1);
        asField = asField.withFieldName(attributeNameResolver + fieldName).get<FieldAccessLogicalFunction>();
    }}
    asField = asField.withDataType(getFinalAggregateStamp()).get<FieldAccessLogicalFunction>();
    inputStamp = onField.getDataType();
}}

NES::SerializableAggregationFunction {nebula_name}AggregationLogicalFunction::serialize() const
{{
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(valueField, timestampField, valueField, asField);
    saf.set_type(std::string(NAME));
    return saf;
}}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{{
    // serializeTemporalSequence only has a 4-field (lon, lat, ts, as) form, so
    // the two-field (value, ts) shape packs the value field twice; fields[2] is
    // that duplicate and is ignored here — the alias is fields[3].
    if (arguments.fields.size() == 4)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[3]);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires value, timestamp, and alias fields but got {{}}",
        arguments.fields.size());
}}

}} // namespace NES
"""

# Physical-layer .hpp templates.
PHYSICAL_HPP_TGEO = """\
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
{{

class {nebula_name}AggregationPhysicalFunction : public AggregationPhysicalFunction
{{
public:
    {nebula_name}AggregationPhysicalFunction(
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
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~{nebula_name}AggregationPhysicalFunction() override = default;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    PhysicalFunction lonFunction;
    PhysicalFunction latFunction;
    PhysicalFunction timestampFunction;
}};

}}
"""

PHYSICAL_HPP_TNUMBER = """\
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
{{

class {nebula_name}AggregationPhysicalFunction : public AggregationPhysicalFunction
{{
public:
    {nebula_name}AggregationPhysicalFunction(
        DataType inputType,
        DataType resultType,
        PhysicalFunction valueFunctionParam,
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
    Nautilus::Record lower(nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider) override;
    void reset(nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider& pipelineMemoryProvider) override;
    [[nodiscard]] size_t getSizeOfStateInBytes() const override;
    ~{nebula_name}AggregationPhysicalFunction() override = default;
    void cleanup(nautilus::val<AggregationState*> aggregationState) override;

private:
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef;
    PhysicalFunction valueFunction;
    PhysicalFunction timestampFunction;
}};

}}
"""

# Physical .cpp templates — the core logic. lift/combine/reset/cleanup are identical
# scaffold; lower() is the per-op differential (builds trajectory string + MEOS call).
PHYSICAL_CPP_TGEO = """\
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

#include <Aggregation/Function/Meos/{nebula_name}AggregationPhysicalFunction.hpp>

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
extern "C" {{
#include <meos.h>
#include <meos_geo.h>
}}

namespace NES
{{

constexpr static std::string_view LonFieldName = "lon";
constexpr static std::string_view LatFieldName = "lat";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex {mutex_name};


{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{{
}}

void {nebula_name}AggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({{
        {{std::string(LonFieldName), lonValue}},
        {{std::string(LatFieldName), latValue}},
        {{std::string(TimestampFieldName), timestampValue}}
    }});

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}}

void {nebula_name}AggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{{
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        {{ vector1->copyFrom(*vector2); }},
        memArea1,
        memArea2);
}}

Nautilus::Record {nebula_name}AggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{{
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {{
            return pagedVector->getTotalNumberOfEntries();
        }},
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<{return_cpp_type}>(0));
        return resultRecord;
    }}

    auto trajectoryStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {{
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 150 + 50;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "{{");
            return buffer;
        }},
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;

        const auto lonValue = itemRecord.read(std::string(LonFieldName));
        const auto latValue = itemRecord.read(std::string(LatFieldName));
        const auto timestampValue = itemRecord.read(std::string(TimestampFieldName));

        auto lon = lonValue.cast<nautilus::val<double>>();
        auto lat = latValue.cast<nautilus::val<double>>();
        auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

        trajectoryStr = nautilus::invoke(
            +[](char* buffer, double lonVal, double latVal, int64_t tsVal, int64_t counter) -> char*
            {{
                if (counter > 0) {{
                    strcat(buffer, ", ");
                }}

                long long adjustedTime;
                if (tsVal > 1000000000000LL) {{
                    adjustedTime = tsVal / 1000;
                }} else {{
                    adjustedTime = tsVal;
                }}

                std::string timestampString = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                const char* timestampStr = timestampString.c_str();

                char pointStr[120];
                sprintf(pointStr, "Point(%.6f %.6f)@%s", lonVal, latVal, timestampStr);
                strcat(buffer, pointStr);
                return buffer;
            }},
            trajectoryStr,
            lon,
            lat,
            timestamp,
            pointCounter);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }}

    trajectoryStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {{
            strcat(buffer, "}}");
            return buffer;
        }},
        trajectoryStr);

    auto resultValue = nautilus::invoke(
        +[](const char* trajStr) -> {return_cpp_type}
        {{
            if (!trajStr || strlen(trajStr) == 0) {{
                free((void*)trajStr);
                return ({return_cpp_type})0;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!temp) {{
                free((void*)trajStr);
                return ({return_cpp_type})0;
            }}

            {value_compute}

            MEOS::Meos::freeTemporalObject(temp);
            free((void*)trajStr);
            return value;
        }},
        trajectoryStr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;
}}

void {nebula_name}AggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        }},
        aggregationState);
}}

size_t {nebula_name}AggregationPhysicalFunction::getSizeOfStateInBytes() const
{{
    return sizeof(Nautilus::Interface::PagedVector);
}}

void {nebula_name}AggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        }},
        aggregationState);
}}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{{
    throw std::runtime_error("{class_name_token} aggregation cannot be created through the registry. "
                             "It requires three field functions (longitude, latitude, timestamp)");
}}

}} // namespace NES
"""

PHYSICAL_CPP_TNUMBER = """\
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

#include <Aggregation/Function/Meos/{nebula_name}AggregationPhysicalFunction.hpp>

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
extern "C" {{
#include <meos.h>
}}

namespace NES
{{

constexpr static std::string_view ValueFieldName = "value";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex {mutex_name};


{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
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
{{
}}

void {nebula_name}AggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({{
        {{std::string(ValueFieldName), valueValue}},
        {{std::string(TimestampFieldName), timestampValue}}
    }});

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}}

void {nebula_name}AggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{{
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        {{ vector1->copyFrom(*vector2); }},
        memArea1,
        memArea2);
}}

Nautilus::Record {nebula_name}AggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{{
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {{
            return pagedVector->getTotalNumberOfEntries();
        }},
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<{return_cpp_type}>(0));
        return resultRecord;
    }}

    auto sequenceStr = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector) -> char*
        {{
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 80 + 50;
            char* buffer = (char*)malloc(bufferSize);
            memset(buffer, 0, bufferSize);
            strcpy(buffer, "{{");
            return buffer;
        }},
        pagedVectorPtr);

    auto pointCounter = nautilus::val<int64_t>(0);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;

        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        const auto timestampRaw = itemRecord.read(std::string(TimestampFieldName));

        auto value = valueRaw.cast<nautilus::val<{lift_value_cpp_type}>>();
        auto timestamp = timestampRaw.cast<nautilus::val<int64_t>>();

        sequenceStr = nautilus::invoke(
            +[](char* buffer, {lift_value_cpp_type} valueVal, int64_t tsVal, int64_t counter) -> char*
            {{
                if (counter > 0) {{
                    strcat(buffer, ", ");
                }}

                long long adjustedTime;
                if (tsVal > 1000000000000LL) {{
                    adjustedTime = tsVal / 1000;
                }} else {{
                    adjustedTime = tsVal;
                }}

                std::string timestampString = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                const char* timestampStr = timestampString.c_str();

                char itemStr[80];
                sprintf(itemStr, "{value_printf_fmt}@%s", valueVal, timestampStr);
                strcat(buffer, itemStr);
                return buffer;
            }},
            sequenceStr,
            value,
            timestamp,
            pointCounter);

        pointCounter = pointCounter + nautilus::val<int64_t>(1);
    }}

    sequenceStr = nautilus::invoke(
        +[](char* buffer) -> char*
        {{
            strcat(buffer, "}}");
            return buffer;
        }},
        sequenceStr);

    auto resultValue = nautilus::invoke(
        +[](const char* seqStr) -> {return_cpp_type}
        {{
            if (!seqStr || strlen(seqStr) == 0) {{
                free((void*)seqStr);
                return ({return_cpp_type})0;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            Temporal* temp = {tnumber_in_fn}(seqStr);
            if (!temp) {{
                free((void*)seqStr);
                return ({return_cpp_type})0;
            }}

            {return_cpp_type} value = {meos_scalar_fn}(temp);

            free(temp);
            free((void*)seqStr);
            return value;
        }},
        sequenceStr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;
}}

void {nebula_name}AggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        }},
        aggregationState);
}}

size_t {nebula_name}AggregationPhysicalFunction::getSizeOfStateInBytes() const
{{
    return sizeof(Nautilus::Interface::PagedVector);
}}

void {nebula_name}AggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        }},
        aggregationState);
}}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{{
    throw std::runtime_error("{class_name_token} aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}}

}} // namespace NES
"""

# ===========================================================================
# Box-output (VARSIZED) physical .cpp templates.
#
# The 11 MEOS `*_extent_transfn` aggregates do not fold a window to a scalar —
# they fold it to a *box* (a Span / TBox / STBox). NebulaStream emits such a
# windowed value through the same variable-sized-data path that
# TemporalSequenceAggregationPhysicalFunction already uses: in lower() we
# serialize the box to text (`*_out`) and write it as VARSIZED.
#
# To stay byte-identical to the proven scalar templates above (lift / combine /
# reset / cleanup / the trajectory-assembly loop are unchanged), the box
# templates are DERIVED from the scalar templates by swapping exactly two
# well-delimited regions: the empty-window early-return and the finalize tail.
# The swap is asserted (count == 1) so any drift in the scalar template fails
# loudly at import time rather than emitting silently-wrong C++.
# ===========================================================================

# Empty-window early-return — identical in the TGEO and TNUMBER scalar templates.
_EMPTY_SCALAR = """\
    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<{return_cpp_type}>(0));
        return resultRecord;
    }}"""

_EMPTY_BOX = """\
    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        auto emptyVarSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVarSized);
        return resultRecord;
    }}"""

# Finalize tail — TGEO scalar (parseTemporalPoint / trajectoryStr / freeTemporalObject).
_FINALIZE_SCALAR_TGEO = """\
    auto resultValue = nautilus::invoke(
        +[](const char* trajStr) -> {return_cpp_type}
        {{
            if (!trajStr || strlen(trajStr) == 0) {{
                free((void*)trajStr);
                return ({return_cpp_type})0;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            if (!temp) {{
                free((void*)trajStr);
                return ({return_cpp_type})0;
            }}

            {value_compute}

            MEOS::Meos::freeTemporalObject(temp);
            free((void*)trajStr);
            return value;
        }},
        trajectoryStr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;"""

# Finalize tail — TGEO box: fold the windowed trajectory's extent box and emit
# its serialized text as VARSIZED. With a NULL initial state the MEOS extent
# transition fn returns the bbox of the whole-window temporal (e.g.
# tspatial_extent_transfn(NULL, traj) == tspatial_to_stbox(traj)).
_FINALIZE_BOX_TGEO = """\
    auto boxStr = nautilus::invoke(
        +[](const char* trajStr) -> char*
        {{
            if (!trajStr || strlen(trajStr) == 0) {{
                free((void*)trajStr);
                return (char*)nullptr;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            free((void*)trajStr);
            if (!temp) {{
                return (char*)nullptr;
            }}

            {extent_box_type}* aggBox = {extent_transfn}(nullptr, static_cast<Temporal*>(temp));
            MEOS::Meos::freeTemporalObject(temp);
            if (!aggBox) {{
                return (char*)nullptr;
            }}

            char* boxText = {box_out_fn}(aggBox, 15);
            free(aggBox);
            return boxText;
        }},
        trajectoryStr);

    const auto boxStrLen = nautilus::invoke(
        +[](const char* s) -> size_t {{ return s ? strlen(s) : (size_t) 0; }},
        boxStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {{
            if (s) {{
                memcpy(dest, s, len);
                free((void*)s);
            }}
        }},
        variableSized.getContent(),
        boxStr,
        boxStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;"""

# Finalize tail — TNUMBER scalar ({tnumber_in_fn} / sequenceStr / free(temp)).
_FINALIZE_SCALAR_TNUMBER = """\
    auto resultValue = nautilus::invoke(
        +[](const char* seqStr) -> {return_cpp_type}
        {{
            if (!seqStr || strlen(seqStr) == 0) {{
                free((void*)seqStr);
                return ({return_cpp_type})0;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            Temporal* temp = {tnumber_in_fn}(seqStr);
            if (!temp) {{
                free((void*)seqStr);
                return ({return_cpp_type})0;
            }}

            {return_cpp_type} value = {meos_scalar_fn}(temp);

            free(temp);
            free((void*)seqStr);
            return value;
        }},
        sequenceStr);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;"""

# Finalize tail — TNUMBER box.
_FINALIZE_BOX_TNUMBER = """\
    auto boxStr = nautilus::invoke(
        +[](const char* seqStr) -> char*
        {{
            if (!seqStr || strlen(seqStr) == 0) {{
                free((void*)seqStr);
                return (char*)nullptr;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            Temporal* temp = {tnumber_in_fn}(seqStr);
            free((void*)seqStr);
            if (!temp) {{
                return (char*)nullptr;
            }}

            {extent_box_type}* aggBox = {extent_transfn}(nullptr, temp);
            free(temp);
            if (!aggBox) {{
                return (char*)nullptr;
            }}

            char* boxText = {box_out_fn}(aggBox, 15);
            free(aggBox);
            return boxText;
        }},
        sequenceStr);

    const auto boxStrLen = nautilus::invoke(
        +[](const char* s) -> size_t {{ return s ? strlen(s) : (size_t) 0; }},
        boxStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {{
            if (s) {{
                memcpy(dest, s, len);
                free((void*)s);
            }}
        }},
        variableSized.getContent(),
        boxStr,
        boxStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;"""


def _swap_once(template, old, new, what):
    """Replace exactly one occurrence of `old` with `new`, asserting the count
    so a drifted scalar template fails at import rather than emitting bad C++."""
    n = template.count(old)
    if n != 1:
        raise AssertionError(
            f"box-template derivation: expected exactly 1 occurrence of {what}, found {n}")
    return template.replace(old, new)


PHYSICAL_CPP_TGEO_BOX = _swap_once(
    _swap_once(PHYSICAL_CPP_TGEO, _EMPTY_SCALAR, _EMPTY_BOX, "tgeo empty-window block"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_BOX_TGEO, "tgeo finalize tail")

PHYSICAL_CPP_TNUMBER_BOX = _swap_once(
    _swap_once(PHYSICAL_CPP_TNUMBER, _EMPTY_SCALAR, _EMPTY_BOX, "tnumber empty-window block"),
    _FINALIZE_SCALAR_TNUMBER, _FINALIZE_BOX_TNUMBER, "tnumber finalize tail")

# ===========================================================================
# WKB-trajectory output (return_mode "wkb"): materialize the windowed mini-trip
# as a SEQUENCE ([ ... ], linear interpolation — so trajectory functions like
# length are meaningful) and emit its hex-WKB. This is the value the MEOS
# function library composes over (the efficient materialize-once mechanism).
# Derived from the tgeo scalar template by swapping the empty-window write, the
# instant-set braces for sequence brackets, and the finalize.
# ===========================================================================
_FINALIZE_WKB_TGEO = """\
    auto boxStr = nautilus::invoke(
        +[](const char* trajStr) -> char*
        {{
            if (!trajStr || strlen(trajStr) == 0) {{
                free((void*)trajStr);
                return (char*)nullptr;
            }}

            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});

            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            free((void*)trajStr);
            if (!temp) {{
                return (char*)nullptr;
            }}

            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(static_cast<Temporal*>(temp), 0, &hexSize);
            MEOS::Meos::freeTemporalObject(temp);
            return hexOut;
        }},
        trajectoryStr);

    const auto boxStrLen = nautilus::invoke(
        +[](const char* s) -> size_t {{ return s ? strlen(s) : (size_t) 0; }},
        boxStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {{
            if (s) {{
                memcpy(dest, s, len);
                free((void*)s);
            }}
        }},
        variableSized.getContent(),
        boxStr,
        boxStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;"""

PHYSICAL_CPP_TGEO_WKB = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(PHYSICAL_CPP_TGEO, _EMPTY_SCALAR, _EMPTY_BOX, "tgeo-wkb empty-window block"),
            '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tgeo-wkb open bracket -> sequence"),
        '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tgeo-wkb close bracket -> sequence"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_WKB_TGEO, "tgeo-wkb finalize tail")

# ===========================================================================
# Expandable-Temporal* aggregate (return_mode "expand"): the MEOS-native
# streaming model — the aggregate STATE is a live expandable `Temporal*` (a
# mini-trip trajectory), grown in place per event via the public streaming
# primitive `temporal_append_tinstant(..., expand=true)` (amortized-O(1),
# doubling). lower() applies the invariant MEOS scalar fn DIRECTLY to the live
# trajectory — no per-event string build, no parse-the-whole-window, no WKB.
# State is a `Temporal*` slot (sizeof(Temporal*)); public funcs only
# (tgeompoint_in / tsequence_make / temporal_append_tinstant / temporal_merge).
# ===========================================================================
PHYSICAL_CPP_TGEO_EXPAND = """\
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

#include <Aggregation/Function/Meos/{nebula_name}AggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <mutex>
#include <cstring>
#include <cstdio>
#include <string>

#include <MemoryLayout/ColumnLayout.hpp>
#include <Nautilus/Interface/BufferRef/TupleBufferRef.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <nautilus/function.hpp>

#include <AggregationPhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <val.hpp>
#include <val_concepts.hpp>
#include <val_ptr.hpp>

#include <MEOSWrapper.hpp>
extern "C" {{
#include <meos.h>
#include <meos_geo.h>
}}

namespace NES
{{

static std::mutex {mutex_name};


{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{{
}}

void {nebula_name}AggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{{
    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto lon = lonValue.cast<nautilus::val<double>>();
    auto lat = latValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, double lonVal, double latVal, int64_t tsVal) -> void
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[120];
            snprintf(wkt, sizeof(wkt), "SRID=4326;Point(%.6f %.6f)@%s", lonVal, latVal, ts.c_str());

            // Public instant constructor: a single-instant tgeompoint Temporal.
            Temporal* instTemp = tgeompoint_in(wkt);
            if (!instTemp) {{
                return;
            }}
            if (*slot == nullptr) {{
                // First event: a 1-instant sequence; subsequent appendInstant calls
                // grow it in place (expand=true doubles maxcount when full).
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            }} else {{
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }}
            free(instTemp);  // copied by tsequence_make / temporal_append_tinstant
        }},
        aggregationState,
        lon,
        lat,
        timestamp);
}}

void {nebula_name}AggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{{
    nautilus::invoke(
        +[](AggregationState* st1, AggregationState* st2) -> void
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** s1 = reinterpret_cast<Temporal**>(st1);
            Temporal** s2 = reinterpret_cast<Temporal**>(st2);
            if (*s2 == nullptr) {{
                return;
            }}
            if (*s1 == nullptr) {{
                *s1 = *s2;
                *s2 = nullptr;
                return;
            }}
            // temporal_merge returns a fresh temporal (copies inputs, frees nothing).
            Temporal* merged = temporal_merge(*s1, *s2);
            free(*s1);
            free(*s2);
            *s2 = nullptr;
            *s1 = merged;
        }},
        aggregationState1,
        aggregationState2);
}}

Nautilus::Record {nebula_name}AggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{{
    MEOS::Meos::ensureMeosInitialized();

    auto resultValue = nautilus::invoke(
        +[](AggregationState* st) -> {return_cpp_type}
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot == nullptr) {{
                return ({return_cpp_type})0;
            }}
            return {meos_scalar_fn}(*slot);
        }},
        aggregationState);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;
}}

void {nebula_name}AggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        {{
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            *slot = nullptr;
        }},
        aggregationState);
}}

size_t {nebula_name}AggregationPhysicalFunction::getSizeOfStateInBytes() const
{{
    return sizeof(Temporal*);
}}

void {nebula_name}AggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{{
    nautilus::invoke(
        +[](AggregationState* st) -> void
        {{
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot != nullptr) {{
                free(*slot);
                *slot = nullptr;
            }}
        }},
        aggregationState);
}}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{{
    throw std::runtime_error("{class_name_token} aggregation cannot be created through the registry. "
                             "It requires three field functions (longitude, latitude, timestamp)");
}}

}} // namespace NES
"""

# ===========================================================================
# Scalar-fold box-output template (value/time Span extents).
#
# Reuses the tnumber (value, ts) HPP / ctor / lift / combine / reset / cleanup
# verbatim — only lower() differs. There is NO trajectory/sequence string and
# NO MEOS parse: the chosen scalar field is folded DIRECTLY through the MEOS
# extent transition fn (`float_extent_transfn`, `timestamptz_extent_transfn`,
# …), the Span state threading across events as an opaque pointer (NULL initial
# state -> first call allocates via span_make, later calls span_expand in place;
# one allocation total, freed after serialization via the external typed
# wrapper `floatspan_out` / `intspan_out` / `bigintspan_out` / `tstzspan_out`).
# ===========================================================================
PHYSICAL_CPP_SCALARFOLD = """\
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

#include <Aggregation/Function/Meos/{nebula_name}AggregationPhysicalFunction.hpp>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <stdexcept>
#include <utility>
#include <string_view>
#include <cstdlib>
#include <mutex>
#include <cstring>
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
extern "C" {{
#include <meos.h>
}}

namespace NES
{{

constexpr static std::string_view ValueFieldName = "value";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex {mutex_name};


{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
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
{{
}}

void {nebula_name}AggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({{
        {{std::string(ValueFieldName), valueValue}},
        {{std::string(TimestampFieldName), timestampValue}}
    }});

    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    pagedVectorRef.writeRecord(aggregateStateRecord, pipelineMemoryProvider.bufferProvider);
}}

void {nebula_name}AggregationPhysicalFunction::combine(
    const nautilus::val<AggregationState*> aggregationState1,
    const nautilus::val<AggregationState*> aggregationState2,
    PipelineMemoryProvider&)
{{
    const auto memArea1 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState1);
    const auto memArea2 = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState2);

    nautilus::invoke(
        +[](Nautilus::Interface::PagedVector* vector1, const Nautilus::Interface::PagedVector* vector2) -> void
        {{ vector1->copyFrom(*vector2); }},
        memArea1,
        memArea2);
}}

Nautilus::Record {nebula_name}AggregationPhysicalFunction::lower(
    const nautilus::val<AggregationState*> aggregationState, [[maybe_unused]] PipelineMemoryProvider& pipelineMemoryProvider)
{{
    MEOS::Meos::ensureMeosInitialized();

    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);
    const Nautilus::Interface::PagedVectorRef pagedVectorRef(pagedVectorPtr, bufferRef);
    const auto allFieldNames = bufferRef->getMemoryLayout()->getSchema().getFieldNames();
    const auto numberOfEntries = invoke(
        +[](const Nautilus::Interface::PagedVector* pagedVector)
        {{
            return pagedVector->getTotalNumberOfEntries();
        }},
        pagedVectorPtr);

    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        auto emptyVarSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVarSized);
        return resultRecord;
    }}

    // Fold the windowed scalar field through the MEOS extent transition fn.
    // The Span state threads across events as an opaque pointer; a NULL initial
    // state makes the first call allocate, later calls expand in place.
    auto spanState = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector*) -> void* {{ return nullptr; }},
        pagedVectorPtr);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        auto value = valueRaw.cast<nautilus::val<{fold_field_cpp_type}>>();

        spanState = nautilus::invoke(
            +[](void* state, {fold_field_cpp_type} val) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
                {fold_invoke_body}
            }},
            spanState,
            value);
    }}

    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{
                return (char*)nullptr;
            }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Span* sp = static_cast<Span*>(state);
            char* out = {box_out_call};
            free(state);
            return out;
        }},
        spanState);

    const auto boxStrLen = nautilus::invoke(
        +[](const char* s) -> size_t {{ return s ? strlen(s) : (size_t) 0; }},
        boxStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(boxStrLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {{
            if (s) {{
                memcpy(dest, s, len);
                free((void*)s);
            }}
        }},
        variableSized.getContent(),
        boxStr,
        boxStrLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;
}}

void {nebula_name}AggregationPhysicalFunction::reset(const nautilus::val<AggregationState*> aggregationState, PipelineMemoryProvider&)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            new (pagedVector) Nautilus::Interface::PagedVector();
        }},
        aggregationState);
}}

size_t {nebula_name}AggregationPhysicalFunction::getSizeOfStateInBytes() const
{{
    return sizeof(Nautilus::Interface::PagedVector);
}}

void {nebula_name}AggregationPhysicalFunction::cleanup(nautilus::val<AggregationState*> aggregationState)
{{
    nautilus::invoke(
        +[](AggregationState* pagedVectorMemArea) -> void
        {{
            auto* pagedVector = reinterpret_cast<Nautilus::Interface::PagedVector*>(pagedVectorMemArea);
            pagedVector->~PagedVector();
        }},
        aggregationState);
}}


AggregationPhysicalFunctionRegistryReturnType AggregationPhysicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationPhysicalFunction(
    AggregationPhysicalFunctionRegistryArguments)
{{
    throw std::runtime_error("{class_name_token} aggregation cannot be created through the registry. "
                             "It requires two field functions (value, timestamp)");
}}

}} // namespace NES
"""

# ===========================================================================
# Set-collect aggregate template (windowed union -> Set).
#
# Same scalar-fold mechanism as PHYSICAL_CPP_SCALARFOLD, but the per-event
# `*_union_transfn` accumulates an unordered Set state (not a Span); the window
# is finalized with `set_union_finalfn` into the canonical Set before
# serialization through an external typed wrapper (floatset_out / intset_out /
# bigintset_out / tstzset_out). Derived from the scalar-fold template by an
# asserted swap of only the serialize lambda — the fold loop / lift / combine /
# reset / cleanup stay byte-identical.
# ===========================================================================
_SCALARFOLD_SERIALIZE_SPAN = """\
    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{
                return (char*)nullptr;
            }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Span* sp = static_cast<Span*>(state);
            char* out = {box_out_call};
            free(state);
            return out;
        }},
        spanState);"""

_SCALARFOLD_SERIALIZE_SET = """\
    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{
                return (char*)nullptr;
            }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            // set_union_finalfn pfree()s the state internally and returns a new
            // Set, so the state must NOT be freed again here (double free).
            Set* sp = {finalfn}(static_cast<Set*>(state));
            if (!sp) {{
                return (char*)nullptr;
            }}
            char* out = {box_out_call};
            free(sp);
            return out;
        }},
        spanState);"""

PHYSICAL_CPP_SETFOLD = _swap_once(
    PHYSICAL_CPP_SCALARFOLD, _SCALARFOLD_SERIALIZE_SPAN, _SCALARFOLD_SERIALIZE_SET,
    "scalarfold serialize -> setfold (finalfn)")

# ===========================================================================
# Parser-glue templates: TWO dispatch sites in AntlrSQLQueryPlanCreator.cpp.
# Site 1 is the dedicated-token case-switch (~line 965 in mariana's tree).
# Site 2 is the IDENTIFIER fallback `else if (funcName == "TOKEN")` chain
# (~line 2062 in mariana's tree).
# ===========================================================================

# Site 1 — case-switch dispatch. Two shapes (tgeo 3-arg, tnumber 2-arg).
CASE_SWITCH_TGEO = """\
        /* BEGIN CODEGEN AGGREGATION GLUE: {sql_token} (case-switch) */
        case AntlrSQLLexer::{sql_token}:
            // {comment_one_liner}
            if (helpers.top().functionBuilder.size() != 3) {{
                throw InvalidQuerySyntax("{sql_token} requires exactly three arguments (longitude, latitude, timestamp), but got {{}}", helpers.top().functionBuilder.size());
            }}
            {{
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {{
                    throw InvalidQuerySyntax("{sql_token} arguments must be field references");
                }}

                helpers.top().windowAggs.push_back(
                    {nebula_name}AggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }}
            break;
        /* END CODEGEN AGGREGATION GLUE: {sql_token} (case-switch) */
"""

CASE_SWITCH_TNUMBER = """\
        /* BEGIN CODEGEN AGGREGATION GLUE: {sql_token} (case-switch) */
        case AntlrSQLLexer::{sql_token}:
            // {comment_one_liner}
            if (helpers.top().functionBuilder.size() != 2) {{
                throw InvalidQuerySyntax("{sql_token} requires exactly two arguments (value, timestamp), but got {{}}", helpers.top().functionBuilder.size());
            }}
            {{
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto valueFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!valueFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {{
                    throw InvalidQuerySyntax("{sql_token} arguments must be field references");
                }}

                helpers.top().windowAggs.push_back(
                    {nebula_name}AggregationLogicalFunction::create(valueFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(valueFunction);
            }}
            break;
        /* END CODEGEN AGGREGATION GLUE: {sql_token} (case-switch) */
"""

# Site 2 — funcName == "TOKEN" string chain.
FUNCNAME_CHAIN_TGEO = """\
            /* BEGIN CODEGEN AGGREGATION GLUE: {sql_token} (funcName chain) */
            else if (funcName == "{sql_token}")
            {{
                if (helpers.top().functionBuilder.size() < 3)
                {{
                    throw InvalidQuerySyntax("{sql_token} requires three arguments at {{}}", context->getText());
                }}
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back({nebula_name}AggregationLogicalFunction::create(lon, lat, ts));
            }}
            /* END CODEGEN AGGREGATION GLUE: {sql_token} (funcName chain) */
"""

FUNCNAME_CHAIN_TNUMBER = """\
            /* BEGIN CODEGEN AGGREGATION GLUE: {sql_token} (funcName chain) */
            else if (funcName == "{sql_token}")
            {{
                if (helpers.top().functionBuilder.size() < 2)
                {{
                    throw InvalidQuerySyntax("{sql_token} requires two arguments at {{}}", context->getText());
                }}
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto value = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back({nebula_name}AggregationLogicalFunction::create(value, ts));
            }}
            /* END CODEGEN AGGREGATION GLUE: {sql_token} (funcName chain) */
"""

# Site 3 — optimizer logical→physical lowering rule.
OPTIMIZER_LOWERING_TGEO = """\
        /* BEGIN CODEGEN AGGREGATION GLUE: {class_name_token} (optimizer lowering) */
        if (name == std::string_view("{class_name_token}"))
        {{
            auto specificDescriptor = std::dynamic_pointer_cast<{nebula_name}AggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected {nebula_name}AggregationLogicalFunction for {class_name_token}");

            auto lonPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLonField());
            auto latPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getLatField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("lon", specificDescriptor->getLonField().getDataType());
            stateSchema.addField("lat", specificDescriptor->getLatField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<{nebula_name}AggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                lonPF,
                latPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }}
        /* END CODEGEN AGGREGATION GLUE: {class_name_token} (optimizer lowering) */
"""

OPTIMIZER_LOWERING_TNUMBER = """\
        /* BEGIN CODEGEN AGGREGATION GLUE: {class_name_token} (optimizer lowering) */
        if (name == std::string_view("{class_name_token}"))
        {{
            auto specificDescriptor = std::dynamic_pointer_cast<{nebula_name}AggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected {nebula_name}AggregationLogicalFunction for {class_name_token}");

            auto valuePF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getValueField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("value", specificDescriptor->getValueField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<{nebula_name}AggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                valuePF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }}
        /* END CODEGEN AGGREGATION GLUE: {class_name_token} (optimizer lowering) */
"""

# ===========================================================================
# Expandable-Temporal* VALUE-OUTPUT: f(live mini-trip) -> Temporal* result,
# serialized to hex-WKB as VARSIZED (the proven box-output VARSIZED tail).
# Derived from PHYSICAL_CPP_TGEO_EXPAND by swapping only the scalar lower() for
# the value-output one. Wires the Temporal-returning single-temporal transforms
# (tgeo_centroid, tpoint_azimuth, tgeompoint_to_tgeometry, …) as windowed
# aggregates over the expandable trajectory.
# ===========================================================================
_EXPAND_LOWER_SCALAR = """\
    auto resultValue = nautilus::invoke(
        +[](AggregationState* st) -> {return_cpp_type}
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot == nullptr) {{
                return ({return_cpp_type})0;
            }}
            return {meos_scalar_fn}(*slot);
        }},
        aggregationState);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, resultValue);
    return resultRecord;"""

_EXPAND_LOWER_WKB = """\
    auto hexStr = nautilus::invoke(
        +[](AggregationState* st) -> char*
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);
            if (*slot == nullptr) {{
                return (char*)nullptr;
            }}
            Temporal* res = {meos_scalar_fn}(*slot);
            if (!res) {{
                return (char*)nullptr;
            }}
            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(res, 0, &hexSize);
            free(res);
            return hexOut;
        }},
        aggregationState);

    const auto hexLen = nautilus::invoke(
        +[](const char* s) -> size_t {{ return s ? strlen(s) : (size_t) 0; }},
        hexStr);

    auto variableSized = pipelineMemoryProvider.arena.allocateVariableSizedData(hexLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, size_t len) -> void
        {{
            if (s) {{
                memcpy(dest, s, len);
                free((void*)s);
            }}
        }},
        variableSized.getContent(),
        hexStr,
        hexLen);

    Nautilus::Record resultRecord;
    resultRecord.write(resultFieldIdentifier, variableSized);
    return resultRecord;"""

PHYSICAL_CPP_TGEO_EXPAND_WKB = _swap_once(
    PHYSICAL_CPP_TGEO_EXPAND, _EXPAND_LOWER_SCALAR, _EXPAND_LOWER_WKB,
    "expand scalar lower -> value-output (hex-WKB) lower")

# tnumber expandable value-output: same Temporal*-slot lower/reset/cleanup, but
# the per-event instant is a tfloat ("value@ts" via tfloat_in) and the ctor takes
# (value, ts). Derived from the tgeo expand-wkb template by swapping only the ctor
# and lift (the rest — Temporal* slot, appendInstant, value-output finalize — is
# input-shape-independent).
_EXPAND_CTOR_TGEO = """\
{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
    DataType inputType,
    DataType resultType,
    PhysicalFunction lonFunctionParam,
    PhysicalFunction latFunctionParam,
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)
    : AggregationPhysicalFunction(std::move(inputType), std::move(resultType), lonFunctionParam, std::move(resultFieldIdentifier))
    , bufferRef(std::move(bufferRef))
    , lonFunction(std::move(lonFunctionParam))
    , latFunction(std::move(latFunctionParam))
    , timestampFunction(std::move(timestampFunctionParam))
{{
}}"""
_EXPAND_CTOR_TNUMBER = """\
{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
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
{{
}}"""
_EXPAND_LIFT_TGEO = """\
    auto lonValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto latValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto lon = lonValue.cast<nautilus::val<double>>();
    auto lat = latValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, double lonVal, double latVal, int64_t tsVal) -> void
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[120];
            snprintf(wkt, sizeof(wkt), "SRID=4326;Point(%.6f %.6f)@%s", lonVal, latVal, ts.c_str());

            // Public instant constructor: a single-instant tgeompoint Temporal.
            Temporal* instTemp = tgeompoint_in(wkt);
            if (!instTemp) {{
                return;
            }}
            if (*slot == nullptr) {{
                // First event: a 1-instant sequence; subsequent appendInstant calls
                // grow it in place (expand=true doubles maxcount when full).
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            }} else {{
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }}
            free(instTemp);  // copied by tsequence_make / temporal_append_tinstant
        }},
        aggregationState,
        lon,
        lat,
        timestamp);"""
_EXPAND_LIFT_TNUMBER = """\
    auto valueValue = valueFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto value = valueValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, double valueVal, int64_t tsVal) -> void
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[80];
            snprintf(wkt, sizeof(wkt), "%.6f@%s", valueVal, ts.c_str());

            // Public instant constructor: a single-instant tfloat Temporal.
            Temporal* instTemp = tfloat_in(wkt);
            if (!instTemp) {{
                return;
            }}
            if (*slot == nullptr) {{
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            }} else {{
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }}
            free(instTemp);
        }},
        aggregationState,
        value,
        timestamp);"""

PHYSICAL_CPP_TNUMBER_EXPAND_WKB = _swap_once(
    _swap_once(PHYSICAL_CPP_TGEO_EXPAND_WKB, _EXPAND_CTOR_TGEO, _EXPAND_CTOR_TNUMBER, "expand ctor tgeo->tnumber"),
    _EXPAND_LIFT_TGEO, _EXPAND_LIFT_TNUMBER, "expand lift tgeo->tnumber")

# tnpoint expandable value-output: reuses the 3-field tgeo HPP/parser/optimizer
# (the 3 args are rid, frac, ts); only the lift (NPoint instant via tnpoint_in)
# and the npoint include change. Wires tnpoint trajectory transforms over the
# windowed tnpoint mini-series.
_EXPAND_LIFT_TNPOINT = """\
    auto ridValue = lonFunction.execute(record, pipelineMemoryProvider.arena);
    auto fracValue = latFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    auto rid = ridValue.cast<nautilus::val<int64_t>>();
    auto frac = fracValue.cast<nautilus::val<double>>();
    auto timestamp = timestampValue.cast<nautilus::val<int64_t>>();

    nautilus::invoke(
        +[](AggregationState* st, int64_t ridVal, double fracVal, int64_t tsVal) -> void
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal** slot = reinterpret_cast<Temporal**>(st);

            long long sec = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
            std::string ts = MEOS::Meos::convertSecondsToTimestamp(sec);
            char wkt[80];
            snprintf(wkt, sizeof(wkt), "NPoint(%lld,%.6f)@%s", (long long) ridVal, fracVal, ts.c_str());

            Temporal* instTemp = tnpoint_in(wkt);
            if (!instTemp) {{
                return;
            }}
            if (*slot == nullptr) {{
                TInstant* arr[1];
                arr[0] = (TInstant*) instTemp;
                *slot = (Temporal*) tsequence_make((TInstant**) arr, 1, true, true, LINEAR, false);
            }} else {{
                *slot = temporal_append_tinstant(*slot, (const TInstant*) instTemp, LINEAR, 0.0, nullptr, true);
            }}
            free(instTemp);
        }},
        aggregationState,
        rid,
        frac,
        timestamp);"""

PHYSICAL_CPP_TNPOINT_EXPAND_WKB = _swap_once(
    _swap_once(PHYSICAL_CPP_TGEO_EXPAND_WKB, _EXPAND_LIFT_TGEO, _EXPAND_LIFT_TNPOINT, "expand lift tgeo->tnpoint"),
    "#include <meos_geo.h>", "#include <meos_geo.h>\n#include <meos_npoint.h>", "tnpoint include")


# ===========================================================================
# Shape dispatchers + emit_operator.
# ===========================================================================

def physical_template_for(op):
    box = op.get("return_mode") == "box"
    if op["input_shape"] == "tgeo":
        if op.get("return_mode") == "wkb":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_WKB
        if op.get("return_mode") == "expand":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_EXPAND
        if op.get("return_mode") == "expand_wkb":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_EXPAND_WKB
        if op.get("return_mode") == "expand_wkb_tnpoint":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TNPOINT_EXPAND_WKB
        return PHYSICAL_HPP_TGEO, (PHYSICAL_CPP_TGEO_BOX if box else PHYSICAL_CPP_TGEO)
    if op["input_shape"] == "tnumber":
        # Scalar-fold reuses the tnumber (value, ts) HPP but folds the field
        # directly through the MEOS extent transition fn (no string / no parse);
        # set-collect is the same shape with a Set state + a union finalfn.
        if op.get("return_mode") == "expand_wkb":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_TNUMBER_EXPAND_WKB
        if op.get("fold") == "scalar":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_SCALARFOLD
        if op.get("fold") == "set":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_SETFOLD
        return PHYSICAL_HPP_TNUMBER, (PHYSICAL_CPP_TNUMBER_BOX if box else PHYSICAL_CPP_TNUMBER)
    raise ValueError(f"unknown input_shape: {op['input_shape']}")


def logical_template_for(op):
    if op["input_shape"] == "tgeo":
        return LOGICAL_HPP_TGEO, LOGICAL_CPP_TGEO
    if op["input_shape"] == "tnumber":
        return LOGICAL_HPP_TNUMBER, LOGICAL_CPP_TNUMBER
    raise ValueError(f"unknown input_shape: {op['input_shape']}")


def case_switch_template_for(op):
    return CASE_SWITCH_TGEO if op["input_shape"] == "tgeo" else CASE_SWITCH_TNUMBER


def funcname_chain_template_for(op):
    return FUNCNAME_CHAIN_TGEO if op["input_shape"] == "tgeo" else FUNCNAME_CHAIN_TNUMBER


def optimizer_lowering_template_for(op):
    return OPTIMIZER_LOWERING_TGEO if op["input_shape"] == "tgeo" else OPTIMIZER_LOWERING_TNUMBER


def emit_operator(op, output_root: Path):
    nebula_name = op["nebula_name"]
    logical_hpp_tmpl, logical_cpp_tmpl = logical_template_for(op)
    physical_hpp_tmpl, physical_cpp_tmpl = physical_template_for(op)

    # Common substitution dict.
    fmt = {
        "nebula_name":         nebula_name,
        "class_name_token":    op["class_name_token"],
        "sql_token":           op["sql_token"],
        "comment_one_liner":   op["comment_one_liner"],
        "meos_scalar_fn":      op.get("meos_scalar_fn", ""),
        "return_cpp_type":     op.get("return_cpp_type", "double"),
        "final_stamp_type":    op["final_stamp_type"],
        "mutex_name":          f"meos_{nebula_name.lower()}_mutex",
        # tnumber-only extras (harmless for tgeo since unused)
        "lift_value_cpp_type": op.get("lift_value_cpp_type", "double"),
        "value_printf_fmt":    op.get("value_printf_fmt", "%.6f"),
        "tnumber_in_fn":       op.get("tnumber_in_fn", "tfloat_in"),
        # box-output (VARSIZED extent) extras — only referenced by the *_BOX
        # physical templates; harmless for scalar ops.
        "extent_transfn":      op.get("extent_transfn", ""),
        "extent_box_type":     op.get("extent_box_type", "STBox"),
        "box_out_fn":          op.get("box_out_fn", ""),
        # scalar-fold / set-collect extras — referenced by the *FOLD templates.
        "fold_field_cpp_type": op.get("fold_field_cpp_type", "double"),
        "fold_invoke_body":    op.get("fold_invoke_body", ""),
        "box_out_call":        op.get("box_out_call", ""),
        "finalfn":             op.get("finalfn", ""),
    }

    # value_compute (point/tgeo finalize): either fold the windowed sequence
    # directly with meos_scalar_fn, or — for the EXTENT shape — first reduce the
    # sequence to its bounding box (tspatial_to_stbox / ...) and apply a box
    # accessor/predicate to that windowed extent. In box-output mode the
    # finalize is the serialized extent box itself (no value_compute).
    box_build = op.get("extent_box_build_fn")
    if op.get("return_mode") in ("box", "wkb"):
        fmt["value_compute"] = ""
    elif box_build:
        box_t = op.get("extent_box_type", "STBox")
        fmt["value_compute"] = (
            f'{box_t}* aggBox = {box_build}(static_cast<Temporal*>(temp));\n'
            f'            {op["return_cpp_type"]} value = aggBox ? '
            f'{op["meos_scalar_fn"]}(aggBox) : ({op["return_cpp_type"]})0;\n'
            f'            if (aggBox) free(aggBox);')
    else:
        fmt["value_compute"] = (
            f'{op["return_cpp_type"]} value = '
            f'{op["meos_scalar_fn"]}(static_cast<Temporal*>(temp));')

    paths = {
        "logical_hpp":  output_root / "nes-logical-operators/include/Operators/Windows/Aggregations/Meos" / f"{nebula_name}AggregationLogicalFunction.hpp",
        "logical_cpp":  output_root / "nes-logical-operators/src/Operators/Windows/Aggregations/Meos" / f"{nebula_name}AggregationLogicalFunction.cpp",
        "physical_hpp": output_root / "nes-physical-operators/include/Aggregation/Function/Meos" / f"{nebula_name}AggregationPhysicalFunction.hpp",
        "physical_cpp": output_root / "nes-physical-operators/src/Aggregation/Function/Meos" / f"{nebula_name}AggregationPhysicalFunction.cpp",
    }
    for p in paths.values():
        p.parent.mkdir(parents=True, exist_ok=True)

    paths["logical_hpp"].write_text(logical_hpp_tmpl.format(**fmt))
    paths["logical_cpp"].write_text(logical_cpp_tmpl.format(**fmt))
    paths["physical_hpp"].write_text(physical_hpp_tmpl.format(**fmt))
    paths["physical_cpp"].write_text(physical_cpp_tmpl.format(**fmt))
    sys.stderr.write(f"  ✓ {nebula_name}: emitted 4 files\n")


# ===========================================================================
# Idempotent injectors.
# ===========================================================================

def inject_cmake_entries(operators, output_root: Path) -> int:
    """Append per-op `add_plugin(...)` entries to both layers' aggregation
    CMakeLists. Idempotent: skips entries already present."""
    n_added = 0
    # Layer (logical | physical) → (CMakeLists path, plugin suffix)
    layers = [
        ("logical",  output_root / "nes-logical-operators/src/Operators/Windows/Aggregations/Meos/CMakeLists.txt",  "Logical"),
        ("physical", output_root / "nes-physical-operators/src/Aggregation/Function/Meos/CMakeLists.txt",          "Physical"),
    ]
    for label, cml, suffix in layers:
        if not cml.exists():
            sys.stderr.write(f"  ! cmake-entries: {cml} not found, skipping {label}\n")
            continue
        body = cml.read_text()
        new_lines = []
        for op in operators:
            # Target name must NOT include "Aggregation" suffix — the registry codegen
            # appends "Aggregation<RegistryKind>" itself, so a "...Aggregation" target
            # would yield a double-Aggregation symbol. Mariana's convention is the
            # target name = the SQL-side aggregation name (e.g. "TemporalLength"),
            # NOT the C++ class basename. We follow that.
            target_name = op["nebula_name"]
            suffix_kind = "AggregationLogicalFunction" if label == "logical" else "AggregationPhysicalFunction"
            registry_kind = "AggregationLogicalFunction" if label == "logical" else "AggregationPhysicalFunction"
            cpp_basename = f"{op['nebula_name']}{suffix_kind}.cpp"
            entry = (
                f"add_plugin({target_name} {registry_kind} "
                f"nes-{label}-operators {cpp_basename})"
            )
            # Match by basename to be tolerant of formatting drift
            marker = f"add_plugin({target_name} {registry_kind}"
            if marker in body:
                continue
            new_lines.append(entry)
        if new_lines:
            with cml.open("a") as f:
                f.write("\n".join(new_lines) + "\n")
            sys.stderr.write(f"  ✓ cmake-entries ({label}): appended {len(new_lines)} entry(ies)\n")
            n_added += len(new_lines)
    return n_added


def inject_g4(operators, g4_path: Path) -> int:
    """Inject lexer-token + functionName alternation entries into AntlrSQL.g4."""
    if not g4_path.exists():
        sys.stderr.write(f"  ! g4: {g4_path} not found, skipping\n")
        return 0
    body = g4_path.read_text()
    n_added = 0

    new_tokens = []
    for op in operators:
        tok = op["sql_token"]
        if re.search(rf"^{re.escape(tok)}\s*:", body, re.MULTILINE):
            continue
        new_tokens.append(f"{tok}: '{tok}' | '{tok.lower()}';")
    if new_tokens:
        if "/* BEGIN CODEGEN AGGREGATION LEXER TOKENS */" in body:
            body = re.sub(
                r"(/\* BEGIN CODEGEN AGGREGATION LEXER TOKENS \*/\n)(.*?)(/\* END CODEGEN AGGREGATION LEXER TOKENS \*/)",
                lambda mm: mm.group(1) + mm.group(2) + "\n".join(new_tokens) + "\n" + mm.group(3),
                body, count=1, flags=re.DOTALL,
            )
        else:
            anchor_re = re.compile(r"^WATERMARK:.*$", re.MULTILINE)
            m = anchor_re.search(body)
            if m is None:
                sys.stderr.write(f"  ! g4: WATERMARK anchor not found\n")
            else:
                insertion = (
                    "/* BEGIN CODEGEN AGGREGATION LEXER TOKENS */\n"
                    + "\n".join(new_tokens)
                    + "\n/* END CODEGEN AGGREGATION LEXER TOKENS */\n"
                )
                body = body[: m.start()] + insertion + body[m.start():]
        n_added += len(new_tokens)
        sys.stderr.write(f"  ✓ g4 lexer-tokens: added {len(new_tokens)} token(s)\n")

    # functionName alternation
    fn_re = re.compile(r"^functionName:\s*([^;]+);", re.MULTILINE)
    m = fn_re.search(body)
    if m is None:
        sys.stderr.write(f"  ! g4: functionName production not found\n")
    else:
        alternation = m.group(1)
        new_alts = []
        for op in operators:
            tok = op["sql_token"]
            if re.search(rf"\b{re.escape(tok)}\b", alternation):
                continue
            new_alts.append(tok)
        if new_alts:
            new_alt_text = alternation.rstrip() + " | " + " | ".join(new_alts)
            body = body[: m.start()] + f"functionName:  {new_alt_text};" + body[m.end():]
            sys.stderr.write(f"  ✓ g4 functionName: added {len(new_alts)} alternative(s)\n")

    g4_path.write_text(body)
    return n_added


def inject_parser_cpp(operators, cpp_path: Path) -> int:
    """Inject TWO dispatch sites + per-op #include."""
    if not cpp_path.exists():
        sys.stderr.write(f"  ! parser-cpp: {cpp_path} not found, skipping\n")
        return 0
    body = cpp_path.read_text()
    n_added = 0

    # 1) #includes — insert after the LAST `#include <Operators/Windows/Aggregations/Meos/...>` line.
    new_includes = []
    for op in operators:
        inc = f"#include <Operators/Windows/Aggregations/Meos/{op['nebula_name']}AggregationLogicalFunction.hpp>"
        if inc in body:
            continue
        new_includes.append(inc)
    if new_includes:
        agg_inc_re = re.compile(r"(^#include <Operators/Windows/Aggregations/Meos/[^>]+>\s*\n)+", re.MULTILINE)
        matches = list(agg_inc_re.finditer(body))
        if matches:
            last = matches[-1]
            body = body[: last.end()] + "\n".join(new_includes) + "\n" + body[last.end():]
            sys.stderr.write(f"  ✓ parser-cpp aggregation includes: added {len(new_includes)}\n")
        else:
            # Fall back: insert after any Meos include
            meos_inc_re = re.compile(r"(^#include <Functions/Meos/[^>]+>\s*\n)+", re.MULTILINE)
            matches = list(meos_inc_re.finditer(body))
            if matches:
                last = matches[-1]
                body = body[: last.end()] + "\n".join(new_includes) + "\n" + body[last.end():]
                sys.stderr.write(f"  ✓ parser-cpp aggregation includes (fallback): added {len(new_includes)}\n")
            else:
                sys.stderr.write(f"  ! parser-cpp: no Meos include anchor found\n")

    # 2) Case-switch dispatch — insert after the last `END CODEGEN AGGREGATION GLUE: ... (case-switch)`
    #    marker, else before the `default:` of the switch that contains TGEO_AT_STBOX.
    new_case_blocks = []
    for op in operators:
        tmpl = case_switch_template_for(op)
        marker = f"/* BEGIN CODEGEN AGGREGATION GLUE: {op['sql_token']} (case-switch) */"
        if marker in body:
            continue
        # Skip if pre-existing hand-written case
        if re.search(rf"case\s+AntlrSQLLexer::{re.escape(op['sql_token'])}\s*:", body):
            sys.stderr.write(
                f"  ! parser-cpp: pre-existing case for {op['sql_token']} (case-switch); skipping\n"
            )
            continue
        new_case_blocks.append(tmpl.format(
            sql_token=op["sql_token"], nebula_name=op["nebula_name"], comment_one_liner=op["comment_one_liner"],
        ))
    if new_case_blocks:
        # Anchor preference order:
        #   1. last `END CODEGEN AGGREGATION GLUE: ... (case-switch)` (own marker)
        #   2. last `END CODEGEN PARSER GLUE: ...` (codegen_nebula.py W4.5+)
        #   3. TGEO_AT_STBOX → default: (pre-W4.5 layout)
        last_end_agg = list(re.finditer(r"/\* END CODEGEN AGGREGATION GLUE: [^*]+\(case-switch\)\s*\*/", body))
        last_end_nebula = list(re.finditer(r"/\* END CODEGEN PARSER GLUE: [^*]+\*/", body))
        if last_end_agg:
            insert_at = last_end_agg[-1].end()
            body = body[:insert_at] + "\n" + "\n".join(new_case_blocks) + body[insert_at:]
            sys.stderr.write(f"  ✓ parser-cpp case-switch: added {len(new_case_blocks)} (after own marker)\n")
        elif last_end_nebula:
            insert_at = last_end_nebula[-1].end()
            body = body[:insert_at] + "\n" + "\n".join(new_case_blocks) + body[insert_at:]
            sys.stderr.write(f"  ✓ parser-cpp case-switch: added {len(new_case_blocks)} (after codegen_nebula marker)\n")
        else:
            anchor_re = re.compile(r"(case AntlrSQLLexer::TGEO_AT_STBOX:[\s\S]+?\n\s*break;\n)(\s*default:)")
            m = anchor_re.search(body)
            if m is None:
                sys.stderr.write(f"  ! parser-cpp: no case-switch anchor found\n")
            else:
                insertion = m.group(1) + "\n" + "\n".join(new_case_blocks) + "\n" + m.group(2)
                body = body[: m.start()] + insertion + body[m.end():]
                sys.stderr.write(f"  ✓ parser-cpp case-switch: added {len(new_case_blocks)} (before default:)\n")
        n_added += len(new_case_blocks)

    # 3) funcName-chain dispatch — insert after the last `END CODEGEN AGGREGATION GLUE: ... (funcName chain)`,
    #    else after mariana's CrossDistance else-if block.
    new_chain_blocks = []
    for op in operators:
        tmpl = funcname_chain_template_for(op)
        marker = f"/* BEGIN CODEGEN AGGREGATION GLUE: {op['sql_token']} (funcName chain) */"
        if marker in body:
            continue
        if re.search(rf'funcName == "{re.escape(op["sql_token"])}"', body):
            sys.stderr.write(
                f"  ! parser-cpp: pre-existing funcName chain for {op['sql_token']}; skipping\n"
            )
            continue
        new_chain_blocks.append(tmpl.format(sql_token=op["sql_token"], nebula_name=op["nebula_name"]))
    if new_chain_blocks:
        last_end_re = re.compile(r"/\* END CODEGEN AGGREGATION GLUE: [^*]+\(funcName chain\)\s*\*/")
        ends = list(last_end_re.finditer(body))
        if ends:
            insert_at = ends[-1].end()
            body = body[:insert_at] + "\n" + "\n".join(new_chain_blocks) + body[insert_at:]
            sys.stderr.write(f"  ✓ parser-cpp funcName chain: added {len(new_chain_blocks)} (after marker)\n")
        else:
            anchor_re = re.compile(
                r'(else if \(funcName == "CROSS_DISTANCE"\)[\s\S]+?\n\s*\}\n)',
            )
            m = anchor_re.search(body)
            if m is None:
                sys.stderr.write(f"  ! parser-cpp: no funcName chain anchor (after CROSS_DISTANCE) found\n")
            else:
                insertion = m.group(1) + "\n".join(new_chain_blocks)
                body = body[: m.end()] + "\n".join(new_chain_blocks) + body[m.end():]
                sys.stderr.write(f"  ✓ parser-cpp funcName chain: added {len(new_chain_blocks)} (after CROSS_DISTANCE)\n")
        n_added += len(new_chain_blocks)

    cpp_path.write_text(body)
    return n_added


def inject_optimizer(operators, opt_path: Path) -> int:
    """Inject `if (name == "...")` blocks into LowerToPhysicalWindowedAggregation.cpp."""
    if not opt_path.exists():
        sys.stderr.write(f"  ! optimizer: {opt_path} not found, skipping\n")
        return 0
    body = opt_path.read_text()
    n_added = 0

    # 1) #include for the physical class header
    new_includes = []
    for op in operators:
        inc = f"#include <Aggregation/Function/Meos/{op['nebula_name']}AggregationPhysicalFunction.hpp>"
        if inc in body:
            continue
        new_includes.append(inc)
    # Also need the logical class header
    new_logical_includes = []
    for op in operators:
        inc = f"#include <Operators/Windows/Aggregations/Meos/{op['nebula_name']}AggregationLogicalFunction.hpp>"
        if inc in body:
            continue
        new_logical_includes.append(inc)
    if new_includes or new_logical_includes:
        agg_inc_re = re.compile(r"(^#include <Aggregation/Function/Meos/[^>]+>\s*\n)+", re.MULTILINE)
        matches = list(agg_inc_re.finditer(body))
        if matches:
            last = matches[-1]
            inserts = []
            if new_includes:
                inserts.extend(new_includes)
            if new_logical_includes:
                inserts.extend(new_logical_includes)
            body = body[: last.end()] + "\n".join(inserts) + "\n" + body[last.end():]
            sys.stderr.write(f"  ✓ optimizer includes: added {len(new_includes)} phys + {len(new_logical_includes)} logical\n")
        else:
            sys.stderr.write(f"  ! optimizer: no Aggregation/Function/Meos include anchor found\n")

    # 2) The if-name-match block. Insert after last codegen END marker, else after mariana's CrossDistance block.
    new_blocks = []
    for op in operators:
        tmpl = optimizer_lowering_template_for(op)
        marker = f"/* BEGIN CODEGEN AGGREGATION GLUE: {op['class_name_token']} (optimizer lowering) */"
        if marker in body:
            continue
        # Skip if a pre-existing hand-written block exists for this class_name_token
        if re.search(rf'name == std::string_view\("{re.escape(op["class_name_token"])}"\)', body):
            sys.stderr.write(
                f"  ! optimizer: pre-existing lowering block for {op['class_name_token']}; skipping\n"
            )
            continue
        new_blocks.append(tmpl.format(class_name_token=op["class_name_token"], nebula_name=op["nebula_name"]))
    if new_blocks:
        last_end_re = re.compile(r"/\* END CODEGEN AGGREGATION GLUE: [^*]+\(optimizer lowering\)\s*\*/")
        ends = list(last_end_re.finditer(body))
        if ends:
            insert_at = ends[-1].end()
            body = body[:insert_at] + "\n" + "\n".join(new_blocks) + body[insert_at:]
            sys.stderr.write(f"  ✓ optimizer lowering: added {len(new_blocks)} (after marker)\n")
        else:
            # Anchor: insert just before the "Default path: use registry" comment.
            anchor_re = re.compile(r"(\n\s*// Default path: use registry)")
            m = anchor_re.search(body)
            if m is None:
                sys.stderr.write(f"  ! optimizer: 'Default path' anchor not found\n")
            else:
                insertion = "\n" + "\n".join(new_blocks) + m.group(1)
                body = body[: m.start()] + insertion + body[m.end():]
                sys.stderr.write(f"  ✓ optimizer lowering: added {len(new_blocks)} (before Default path)\n")
        n_added += len(new_blocks)

    opt_path.write_text(body)
    return n_added


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--no-parser-glue", action="store_true")
    parser.add_argument("--no-cmake-entries", action="store_true")
    parser.add_argument("--no-optimizer-glue", action="store_true")
    args = parser.parse_args()

    with open(args.input) as f:
        config = json.load(f)
    operators = config["operators"]

    # The serialized aggregation type (NAME), the optimizer-lowering match, and
    # the registry key must be the same string for the query plan to round-trip
    # (serialize set_type(NAME) -> worker create(type) -> registry key). The
    # registry key is the add_plugin target = nebula_name (PascalCase), so
    # class_name_token (which drives NAME + the optimizer match) MUST equal
    # nebula_name. Earlier specs set it to the SQL token (UPPER_SNAKE), which
    # made create(type) miss the registry and throw UnknownLogicalOperator at
    # deserialize. The SQL spelling lives in sql_token (lexer/parser); it never
    # belongs in NAME. Normalize here so a stray spec value cannot reintroduce
    # the mismatch.
    for op in operators:
        op["class_name_token"] = op["nebula_name"]

    output_root = Path(args.output_root).resolve()
    if not (output_root / "nes-logical-operators").exists():
        sys.exit(f"ERROR: {output_root} does not look like MobilityNebula root")

    sys.stderr.write(f"Emitting {len(operators)} aggregation operator(s):\n\n")
    for op in operators:
        emit_operator(op, output_root)

    if not args.no_cmake_entries:
        sys.stderr.write("\nCMakeLists.txt:\n")
        inject_cmake_entries(operators, output_root)

    if not args.no_parser_glue:
        sys.stderr.write("\nParser glue:\n")
        inject_g4(operators, output_root / "nes-sql-parser/AntlrSQL.g4")
        inject_parser_cpp(operators, output_root / "nes-sql-parser/src/AntlrSQLQueryPlanCreator.cpp")

    if not args.no_optimizer_glue:
        sys.stderr.write("\nOptimizer lowering glue:\n")
        inject_optimizer(operators, output_root / "nes-query-optimizer/src/RewriteRules/LowerToPhysical/LowerToPhysicalWindowedAggregation.cpp")

    sys.stderr.write(f"\nDone. {len(operators) * 4} files emitted.\n")


if __name__ == "__main__":
    main()
