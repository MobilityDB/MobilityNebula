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
``/* BEGIN CODEGEN GLUE: TOKEN */ ... /* END ... */`` markers
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


def meos_family(nebula_name):
    """The toggleable MEOS type family of an aggregation, or None for base types.
    Mirrors codegen_nebula.py: a family aggregation's PHYSICAL op is gated by the
    matching CMake option (CBUFFER/NPOINT/POSE/RGEO) and lives in a per-family
    subdir; the logical op and parser glue stay flat (they never call libmeos).
    The optimizer-lowering glue, which references the physical class directly, is
    wrapped in #if <FAMILY> so a family-OFF build links cleanly."""
    s = nebula_name.lower()
    if "trgeo" in s or "rgeometry" in s:
        return "Rgeo"
    if "pose" in s:
        return "Pose"
    if "npoint" in s or "nsegment" in s:
        return "Npoint"
    if "cbuffer" in s:
        return "Cbuffer"
    return None


# ===========================================================================
# CONSTANT (non-field) argument support for the parameterized tgeo transform
# fold (fold == "tgeotransform"). Each op carries a `const_args` list; each entry
# is {"kind": "..."} (plus op-supplied extras for the `set` kind). The constants
# travel end-to-end as LITERAL STRINGS (parser constantBuilder -> logical
# constArgs -> serde FunctionList/literals -> physical ctor), and are parsed to
# their C value/object only inside the plain-C finalize lambda. This map gives,
# per kind: the C parse statement (templated on the local var name `cN` and the
# source literal `cstr`), and an optional free statement for heap kinds.
# ===========================================================================
_CONST_KIND_PARSE = {
    "double":      ("double {var} = atof({src});", None),
    "int":         ("int {var} = atoi({src});", None),
    "bool":        ("bool {var} = ({src}[0]=='t'||{src}[0]=='T'||{src}[0]=='1');", None),
    "interval":    ("Interval* {var} = interval_in({src}, -1);", "if ({var}) free({var});"),
    "timestamptz": ("TimestampTz {var} = timestamptz_in({src}, -1);", None),
    "interptype":  ("interpType {var} = (interpType) atoi({src});", None),
    "set":         ("Set* {var} = {set_in_fn}({src});", "if ({var}) free({var});"),
}


def render_const_args(op):
    """Build the physical-finalize const fragments for a `const_args` op:
      * const_parse_block : C statements parsing constArgs[i] -> local cN (one per const)
      * const_call_args   : the comma-PREFIXED list of parsed C vars, e.g. ", c0, c1"
      * const_free_block  : C statements freeing any heap consts after the MEOS call
      * num_const_args    : count
    The finalize reads each literal from the captured `constArg0`..`constArgN` C
    strings; render_const_args emits parse stmts referencing those names."""
    consts = op.get("const_args", []) or []
    parse_lines = []
    free_lines = []
    call_args = []
    lambda_params = []
    invoke_args = []
    for i, c in enumerate(consts):
        kind = c["kind"]
        if kind not in _CONST_KIND_PARSE:
            raise ValueError(f"unknown const_args kind: {kind!r} (op {op.get('nebula_name')})")
        parse_tmpl, free_tmpl = _CONST_KIND_PARSE[kind]
        var = f"c{i}"
        src = f"constArg{i}"
        parse_lines.append("            " + parse_tmpl.format(var=var, src=src, set_in_fn=c.get("set_in_fn", "")))
        if free_tmpl:
            free_lines.append("            " + free_tmpl.format(var=var))
        call_args.append(var)
        # The literal string is threaded into the captureless finalize lambda as an
        # extra `const char*` arg, sourced from the constArgs member's c_str() (valid
        # for the synchronous invoke). Same ABI PairMeeting uses for its scalar.
        lambda_params.append(f"const char* {src}")
        invoke_args.append(f"nautilus::val<const char*>(constArgs[{i}].c_str())")
    return {
        "const_parse_block":  "\n".join(parse_lines),
        "const_call_args":    "".join(", " + a for a in call_args),
        "const_free_block":   "\n".join(free_lines),
        "const_lambda_params": "".join(", " + p for p in lambda_params),
        "const_invoke_args":  "".join(",\n        " + a for a in invoke_args),
        "num_const_args":     len(consts),
    }


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

# ===========================================================================
# tpose logical .hpp — FOUR fields (x, y, theta, ts). Mirrors LOGICAL_HPP_TGEO
# but the window is collected as a Pose sequence and folded into a trgeometry.
# ===========================================================================
LOGICAL_HPP_TPOSE = """\
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
 * Four-input (x, y, theta, ts) tpose aggregation. Lift accumulates the events
 * into a paged vector; lower assembles the per-(window, group) pose sequence,
 * converts it to a trgeometry via `geo_tpose_to_trgeometry`, and applies the
 * MEOS sequence accessor `{seq_accessor_call}` to fold it.
 */
class {nebula_name}AggregationLogicalFunction : public WindowAggregationLogicalFunction
{{
public:
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& xField, const FieldAccessLogicalFunction& yField, const FieldAccessLogicalFunction& thetaField, const FieldAccessLogicalFunction& timestampField);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& xField,
        const FieldAccessLogicalFunction& yField,
        const FieldAccessLogicalFunction& thetaField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);

    void inferStamp(const Schema& schema) override;
    ~{nebula_name}AggregationLogicalFunction() override = default;
    [[nodiscard]] NES::SerializableAggregationFunction serialize() const override;
    [[nodiscard]] std::string_view getName() const noexcept override;
    [[nodiscard]] bool requiresSequentialAggregation() const {{ return true; }}

    [[nodiscard]] const FieldAccessLogicalFunction& getXField() const noexcept {{ return xField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getYField() const noexcept {{ return yField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getThetaField() const noexcept {{ return thetaField; }}
    [[nodiscard]] const FieldAccessLogicalFunction& getTimestampField() const noexcept {{ return timestampField; }}

private:
    static constexpr std::string_view NAME = "{class_name_token}";
    static constexpr DataType::Type partialAggregateStampType = DataType::Type::UNDEFINED;
    static constexpr DataType::Type finalAggregateStampType = DataType::Type::{final_stamp_type};

    FieldAccessLogicalFunction xField;
    FieldAccessLogicalFunction yField;
    FieldAccessLogicalFunction thetaField;
    FieldAccessLogicalFunction timestampField;
}};
}}
"""

# tpose logical .cpp — four-field ctor; serialize() uses the 5-arg serde overload.
LOGICAL_CPP_TPOSE = """\
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
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          xField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          xField,
          asField)
    , xField(xField)
    , yField(yField)
    , thetaField(thetaField)
    , timestampField(timestampField)
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(xField, yField, thetaField, timestampField, xField);
}}

std::string_view {nebula_name}AggregationLogicalFunction::getName() const noexcept
{{
    return NAME;
}}

void {nebula_name}AggregationLogicalFunction::inferStamp(const Schema& schema)
{{
    xField = xField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    yField = yField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    thetaField = thetaField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();
    timestampField = timestampField.withInferredDataType(schema).get<FieldAccessLogicalFunction>();

    onField = xField;

    if (!xField.getDataType().isNumeric() || !yField.getDataType().isNumeric()
        || !thetaField.getDataType().isNumeric() || !timestampField.getDataType().isNumeric())
    {{
        throw CannotInferSchema("{nebula_name}AggregationLogicalFunction: x, y, theta, and timestamp fields must be numeric.");
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
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(xField, yField, thetaField, timestampField, asField);
    saf.set_type(std::string(NAME));
    return saf;
}}

AggregationLogicalFunctionRegistryReturnType AggregationLogicalFunctionGeneratedRegistrar::Register{nebula_name}AggregationLogicalFunction(
    AggregationLogicalFunctionRegistryArguments arguments)
{{
    if (arguments.fields.size() == 5)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4]);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires x, y, theta, timestamp, and alias fields but got {{}}",
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

    if ({value_type_guard}!timestampField.getDataType().isNumeric())
    {{
        throw CannotInferSchema("{nebula_name}AggregationLogicalFunction: {value_type_msg}.");
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

# tpose physical .hpp — FOUR PhysicalFunctions (x, y, theta, ts).
PHYSICAL_HPP_TPOSE = """\
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
        PhysicalFunction xFunctionParam,
        PhysicalFunction yFunctionParam,
        PhysicalFunction thetaFunctionParam,
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
    PhysicalFunction xFunction;
    PhysicalFunction yFunction;
    PhysicalFunction thetaFunction;
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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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

# ===========================================================================
# tpose physical .cpp (base) — FOUR fields (x, y, theta, ts). Mirrors
# PHYSICAL_CPP_TGEO but builds a "Pose(Point(x y),theta)@ts" item string and a
# wider per-entry buffer (~200 bytes). The finalize-tail body is kept BYTE-
# IDENTICAL to _FINALIZE_SCALAR_TGEO so the seq-array derivation below can swap
# it via _swap_once (the base is never wired directly — only the SEQ_ARRAY form
# is selected by physical_template_for). The extern "C" block adds meos_pose.h
# (tpose_in) and meos_rgeo.h (geo_tpose_to_trgeometry + trgeometry accessors).
# ===========================================================================
PHYSICAL_CPP_TPOSE = """\
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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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
#include <meos_pose.h>
#include <meos_rgeo.h>
}}

namespace NES
{{

constexpr static std::string_view XFieldName = "x";
constexpr static std::string_view YFieldName = "y";
constexpr static std::string_view ThetaFieldName = "theta";
constexpr static std::string_view TimestampFieldName = "timestamp";

static std::mutex {mutex_name};


{nebula_name}AggregationPhysicalFunction::{nebula_name}AggregationPhysicalFunction(
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
{{
}}

void {nebula_name}AggregationPhysicalFunction::lift(
    const nautilus::val<AggregationState*>& aggregationState, PipelineMemoryProvider& pipelineMemoryProvider, const Nautilus::Record& record)
{{
    const auto pagedVectorPtr = static_cast<nautilus::val<Nautilus::Interface::PagedVector*>>(aggregationState);

    auto xValue = xFunction.execute(record, pipelineMemoryProvider.arena);
    auto yValue = yFunction.execute(record, pipelineMemoryProvider.arena);
    auto thetaValue = thetaFunction.execute(record, pipelineMemoryProvider.arena);
    auto timestampValue = timestampFunction.execute(record, pipelineMemoryProvider.arena);

    Record aggregateStateRecord({{
        {{std::string(XFieldName), xValue}},
        {{std::string(YFieldName), yValue}},
        {{std::string(ThetaFieldName), thetaValue}},
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
            size_t bufferSize = pagedVector->getTotalNumberOfEntries() * 200 + 50;
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

                char poseStr[160];
                sprintf(poseStr, "Pose(Point(%.6f %.6f),%.6f)@%s", xVal, yVal, thetaVal, timestampStr);
                strcat(buffer, poseStr);
                return buffer;
            }},
            trajectoryStr,
            x,
            y,
            theta,
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
                             "It requires four field functions (x, y, theta, timestamp)");
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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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
#include <meos_cbuffer.h>
#include <meos_npoint.h>
#include <meos_pose.h>
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
            char* hexOut = temporal_as_hexwkb(static_cast<Temporal*>(temp), 0x04 /* WKB_EXTENDED */, &hexSize);
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
# Parameterized transform aggregate (fold "tgeotransform"): build the windowed
# CONTINUOUS sequence, apply a MEOS transform that takes the Temporal* PLUS one
# or more CONSTANT (non-field) args, and emit the resulting Temporal* as hex-WKB.
# The constants travel as LITERAL STRINGS: stored on the physical object as a
# std::vector<std::string> `constArgs`, threaded into the captureless finalize
# lambda as extra `nautilus::val<const char*>` invoke args (same ABI PairMeeting
# uses for its scalar dMeet), and parsed to their C type inside the lambda by the
# {const_parse_block} the generator builds from op["const_args"].
#
# Derived from PHYSICAL_CPP_TGEO: empty-window -> VARSIZED, instant-set braces ->
# sequence brackets, ctor gains the constArgs param+member, and the scalar
# finalize tail is replaced by _FINALIZE_WKB_TGEO_CONST.
# ===========================================================================

# ctor: append a constArgs parameter + initializer + member (3 surgical swaps on
# the shared PHYSICAL_CPP_TGEO ctor/lift scaffold).
_CTOR_PARAM_TGEO = """\
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)"""
_CTOR_PARAM_TGEO_CONST = """\
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef,
    std::vector<std::string> constArgsParam)"""

_CTOR_INIT_TGEO = """\
    , timestampFunction(std::move(timestampFunctionParam))
{{
}}"""
_CTOR_INIT_TGEO_CONST = """\
    , timestampFunction(std::move(timestampFunctionParam))
    , constArgs(std::move(constArgsParam))
{{
}}"""

# Finalize tail — parameterized transform: parse the constants, apply the MEOS
# transform fn to the windowed sequence + constants, emit the Temporal* as hex.
_FINALIZE_WKB_TGEO_CONST = """\
    auto boxStr = nautilus::invoke(
        +[](const char* trajStr{const_lambda_params}) -> char*
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

            // Parse each constant literal string to its C value/object.
{const_parse_block}

            Temporal* res = (Temporal*) {meos_scalar_fn}(static_cast<Temporal*>(temp){const_call_args});
            MEOS::Meos::freeTemporalObject(temp);
{const_free_block}
            if (!res) {{
                return (char*)nullptr;
            }}

            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &hexSize);
            free(res);
            return hexOut;
        }},
        trajectoryStr{const_invoke_args});

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

PHYSICAL_CPP_TGEO_CONST = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(
                _swap_once(
                    _swap_once(PHYSICAL_CPP_TGEO, _EMPTY_SCALAR, _EMPTY_BOX, "tgeo-const empty-window block"),
                    '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tgeo-const open bracket -> sequence"),
                '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tgeo-const close bracket -> sequence"),
            _CTOR_PARAM_TGEO, _CTOR_PARAM_TGEO_CONST, "tgeo-const ctor param"),
        _CTOR_INIT_TGEO, _CTOR_INIT_TGEO_CONST, "tgeo-const ctor init"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_WKB_TGEO_CONST, "tgeo-const finalize tail")

# Physical .hpp — append the constArgs ctor param + member to the base tgeo .hpp.
_HPP_CTOR_TGEO = """\
        PhysicalFunction timestampFunctionParam,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef);"""
_HPP_CTOR_TGEO_CONST = """\
        PhysicalFunction timestampFunctionParam,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef,
        std::vector<std::string> constArgsParam);"""
_HPP_MEMBER_TGEO = """\
    PhysicalFunction timestampFunction;
}};"""
_HPP_MEMBER_TGEO_CONST = """\
    PhysicalFunction timestampFunction;
    std::vector<std::string> constArgs;
}};"""

PHYSICAL_HPP_TGEO_CONST = _swap_once(
    _swap_once(
        _swap_once(PHYSICAL_HPP_TGEO, "#include <cstddef>", "#include <cstddef>\n#include <string>\n#include <vector>",
                   "tgeo-const hpp includes"),
        _HPP_CTOR_TGEO, _HPP_CTOR_TGEO_CONST, "tgeo-const hpp ctor param"),
    _HPP_MEMBER_TGEO, _HPP_MEMBER_TGEO_CONST, "tgeo-const hpp member")

# ===========================================================================
# Logical const variant (fold "tgeotransform"): the logical op additionally
# stores the constant literal strings, packs them via serializeConstArgs(), and
# the registrar reconstructs them from arguments.literals. Derived from the base
# tgeo logical templates by surgical swaps so the field-handling scaffold stays
# shared.
# ===========================================================================

# --- HPP swaps ---
_LHPP_CREATE_TGEO = """\
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);"""
_LHPP_CREATE_TGEO_CONST = """\
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& lonField, const FieldAccessLogicalFunction& latField, const FieldAccessLogicalFunction& timestampField, std::vector<std::string> constArgs);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& lonField,
        const FieldAccessLogicalFunction& latField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField,
        std::vector<std::string> constArgs);

    [[nodiscard]] const std::vector<std::string>& getConstArgs() const noexcept {{ return constArgs; }}"""
_LHPP_MEMBER_TGEO = """\
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
}};"""
_LHPP_MEMBER_TGEO_CONST = """\
    FieldAccessLogicalFunction lonField;
    FieldAccessLogicalFunction latField;
    FieldAccessLogicalFunction timestampField;
    std::vector<std::string> constArgs;
}};"""

LOGICAL_HPP_TGEO_CONST = _swap_once(
    _swap_once(
        _swap_once(LOGICAL_HPP_TGEO,
                   "#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>",
                   "#include <string>\n#include <vector>\n#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>",
                   "tgeo-const lhpp includes"),
        _LHPP_CREATE_TGEO, _LHPP_CREATE_TGEO_CONST, "tgeo-const lhpp create/ctor"),
    _LHPP_MEMBER_TGEO, _LHPP_MEMBER_TGEO_CONST, "tgeo-const lhpp member")

# --- CPP swaps ---
_LCPP_INCLUDES_TGEO = "#include <memory>\n#include <string>\n#include <string_view>"
_LCPP_INCLUDES_TGEO_CONST = "#include <memory>\n#include <string>\n#include <string_view>\n#include <utility>\n#include <vector>"

_LCPP_CTOR_TGEO = """\
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
}}"""
_LCPP_CTOR_TGEO_CONST = """\
{nebula_name}AggregationLogicalFunction::{nebula_name}AggregationLogicalFunction(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField,
    std::vector<std::string> constArgs)
    : WindowAggregationLogicalFunction(
          lonField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          lonField,
          asField)
    , lonField(lonField)
    , latField(latField)
    , timestampField(timestampField)
    , constArgs(std::move(constArgs))
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& lonField,
    const FieldAccessLogicalFunction& latField,
    const FieldAccessLogicalFunction& timestampField,
    std::vector<std::string> constArgs)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(lonField, latField, timestampField, lonField, std::move(constArgs));
}}"""

_LCPP_SERIALIZE_TGEO = """\
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(lonField, latField, timestampField, asField);
    saf.set_type(std::string(NAME));
    return saf;"""
_LCPP_SERIALIZE_TGEO_CONST = """\
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(lonField, latField, timestampField, asField);
    TemporalAggregationSerde::serializeConstArgs(saf, constArgs);
    saf.set_type(std::string(NAME));
    return saf;"""

_LCPP_REGISTRAR_TGEO = """\
    if (arguments.fields.size() == 4)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3]);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {{}}",
        arguments.fields.size());"""
_LCPP_REGISTRAR_TGEO_CONST = """\
    if (arguments.fields.size() == 4)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3],
            arguments.literals);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires lon, lat, timestamp, and alias fields but got {{}}",
        arguments.fields.size());"""

LOGICAL_CPP_TGEO_CONST = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(LOGICAL_CPP_TGEO, _LCPP_INCLUDES_TGEO, _LCPP_INCLUDES_TGEO_CONST, "tgeo-const lcpp includes"),
            _LCPP_CTOR_TGEO, _LCPP_CTOR_TGEO_CONST, "tgeo-const lcpp ctor+create"),
        _LCPP_SERIALIZE_TGEO, _LCPP_SERIALIZE_TGEO_CONST, "tgeo-const lcpp serialize"),
    _LCPP_REGISTRAR_TGEO, _LCPP_REGISTRAR_TGEO_CONST, "tgeo-const lcpp registrar")

# ===========================================================================
# Sequence-accessor aggregates (fold "tgeoseq" / "tgeoseqtext"): the windowed
# point stream is built as a CONTINUOUS sequence ("[...]", linear interp) — NOT
# the discrete instant-set ("{...}") the other tgeo aggregates build — because
# the MEOS sequence accessors (temporal_num_sequences / _start_sequence /
# _end_sequence / _sequence_n) require a continuous sequence/sequence-set and
# raise "Input must be a temporal continuous sequence (set)" on a discrete one.
# Two output shapes:
#   tgeoseq      -> a scalar accessor (temporal_num_sequences -> INT32), reusing
#                   the scalar finalize tail (value_compute = meos_scalar_fn(temp)).
#   tgeoseqtext  -> an accessor that returns a TSequence* (start/end/sequence_n),
#                   serialized to text via tspatial_out and emitted as VARSIZED.
#                   The accessor call rides in {seq_accessor_call}.
# Both derive from PHYSICAL_CPP_TGEO by swapping only the brace pair (discrete ->
# continuous); the text variant additionally swaps the scalar finalize tail for a
# parse -> accessor -> tspatial_out -> VARSIZED tail (modeled on _FINALIZE_BOX_TGEO).
# ===========================================================================
_FINALIZE_SEQTEXT_TGEO = _swap_once(
    _FINALIZE_BOX_TGEO,
    """\
            {extent_box_type}* aggBox = {extent_transfn}(nullptr, static_cast<Temporal*>(temp));
            MEOS::Meos::freeTemporalObject(temp);
            if (!aggBox) {{
                return (char*)nullptr;
            }}

            char* boxText = {box_out_fn}(aggBox, 15);
            free(aggBox);
            return boxText;""",
    """\
            Temporal* seqRes = (Temporal*) {seq_accessor_call};
            MEOS::Meos::freeTemporalObject(temp);
            if (!seqRes) {{
                return (char*)nullptr;
            }}

            char* boxText = tspatial_out(seqRes, 6);
            free(seqRes);
            return boxText;""",
    "tgeo box-apply -> sequence-accessor text-out")

PHYSICAL_CPP_TGEO_SEQ = _swap_once(
    _swap_once(
        PHYSICAL_CPP_TGEO,
        '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tgeo-seq open bracket -> continuous"),
    '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tgeo-seq close bracket -> continuous")

PHYSICAL_CPP_TGEO_SEQ_TEXT = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(PHYSICAL_CPP_TGEO, _EMPTY_SCALAR, _EMPTY_BOX, "tgeo-seqtext empty-window block"),
            '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tgeo-seqtext open bracket -> continuous"),
        '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tgeo-seqtext close bracket -> continuous"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_SEQTEXT_TGEO, "tgeo-seqtext finalize tail")

# Array-of-temporal accessor (fold "tgeoseqarray"): same windowed continuous
# sequence as tgeoseqtext, but the accessor returns a HEAP ARRAY of Temporal*
# (T **f(temp, int *count) — temporal_segments / temporal_sequences /
# tpoint_make_simple and the trgeometry instants/segments/sequences). Each
# element is serialized to EWKB hex via temporal_as_hexwkb(elem, 0x04) — the
# SRID-preserving variant, matching the rest of the temporal-out path — then
# freed; the elements are brace-joined into a "{e1, e2, ...}" VARSIZED. The
# accessor call (which writes `_cnt`) rides in {seq_accessor_call}, e.g.
# `temporal_segments(static_cast<Temporal*>(temp), &_cnt)`.
_FINALIZE_SEQARRAY_TGEO = _swap_once(
    _FINALIZE_BOX_TGEO,
    """\
            {extent_box_type}* aggBox = {extent_transfn}(nullptr, static_cast<Temporal*>(temp));
            MEOS::Meos::freeTemporalObject(temp);
            if (!aggBox) {{
                return (char*)nullptr;
            }}

            char* boxText = {box_out_fn}(aggBox, 15);
            free(aggBox);
            return boxText;""",
    """\
            int _cnt = 0;
            void** arr = (void**) {seq_accessor_call};
            MEOS::Meos::freeTemporalObject(temp);
            if (!arr || _cnt <= 0) {{
                if (arr) free(arr);
                return (char*)nullptr;
            }}

            std::string _s = "{{";
            for (int _i = 0; _i < _cnt; _i++) {{
                if (_i) _s += ", ";
                size_t _z = 0;
                char* _e = temporal_as_hexwkb((const Temporal*) arr[_i], 0x04 /* WKB_EXTENDED */, &_z);
                if (_e) {{ _s += _e; free(_e); }}
                free(arr[_i]);
            }}
            _s += "}}";
            free(arr);
            return strdup(_s.c_str());""",
    "tgeo box-apply -> sequence-accessor array-of-temporal hex-out")

PHYSICAL_CPP_TGEO_SEQ_ARRAY = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(PHYSICAL_CPP_TGEO, _EMPTY_SCALAR, _EMPTY_BOX, "tgeo-seqarray empty-window block"),
            '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tgeo-seqarray open bracket -> continuous"),
        '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tgeo-seqarray close bracket -> continuous"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_SEQARRAY_TGEO, "tgeo-seqarray finalize tail")

# ===========================================================================
# tpose array-of-temporal accessor (fold "tposeseqarray"): the windowed
# "[Pose(Point(x y),theta)@ts, ...]" continuous sequence is parsed via tpose_in,
# converted to a trgeometry via geo_tpose_to_trgeometry(refGeom, poseSeq) (the
# proven Nebula path — a unit-square reference geometry, matching the per-event
# Trgeometry* physical functions), then the T**-returning trgeometry accessor
# rides in {seq_accessor_call}; each element is serialized to EWKB hex exactly
# like the tgeo seqarray form. Derived from _FINALIZE_SEQARRAY_TGEO by swapping
# ONLY the `temp` construction (parseTemporalPoint -> tpose_in + convert).
# The whole construction stays inside the same nautilus::invoke lambda (plain C
# context), not nautilus val space. tpose_in / geom_in / geo_tpose_to_trgeometry
# come from meos_pose.h / meos_geo.h / meos_rgeo.h (added to PHYSICAL_CPP_TPOSE).
# ===========================================================================
_FINALIZE_SEQARRAY_TPOSE = _swap_once(
    _FINALIZE_SEQARRAY_TGEO,
    """\
            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            free((void*)trajStr);
            if (!temp) {{
                return (char*)nullptr;
            }}""",
    """\
            std::string trajString(trajStr);
            Temporal* _poseSeq = tpose_in(trajString.c_str());
            free((void*)trajStr);
            if (!_poseSeq) {{
                return (char*)nullptr;
            }}
            GSERIALIZED* _refGeom = geom_in("Polygon((0 0,1 0,1 1,0 1,0 0))", -1);
            if (!_refGeom) {{
                free(_poseSeq);
                return (char*)nullptr;
            }}
            void* temp = geo_tpose_to_trgeometry(_refGeom, _poseSeq);
            free(_refGeom);
            free(_poseSeq);
            if (!temp) {{
                return (char*)nullptr;
            }}""",
    "tpose seqarray temp construction (tpose_in + geo_tpose_to_trgeometry)")

PHYSICAL_CPP_TPOSE_SEQ_ARRAY = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(PHYSICAL_CPP_TPOSE, _EMPTY_SCALAR, _EMPTY_BOX, "tpose-seqarray empty-window block"),
            '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tpose-seqarray open bracket -> continuous"),
        '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tpose-seqarray close bracket -> continuous"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_SEQARRAY_TPOSE, "tpose-seqarray finalize tail")

# ===========================================================================
# tpose single-sequence accessor (fold "tposeseq1"): same windowed
# trgeometry construction as the array form, but the accessor in
# {seq_accessor_call} returns a SINGLE Temporal* (TSequence*) rather than a
# T**+&_cnt array — e.g. trgeometry_start_sequence / trgeometry_end_sequence.
# Serialize that one result directly to EWKB hex (no array loop, no braces).
# Derived from _FINALIZE_SEQARRAY_TPOSE by swapping ONLY the array-loop block.
# ===========================================================================
_FINALIZE_SEQ1_TPOSE = _swap_once(
    _FINALIZE_SEQARRAY_TPOSE,
    """\
            int _cnt = 0;
            void** arr = (void**) {seq_accessor_call};
            MEOS::Meos::freeTemporalObject(temp);
            if (!arr || _cnt <= 0) {{
                if (arr) free(arr);
                return (char*)nullptr;
            }}

            std::string _s = "{{";
            for (int _i = 0; _i < _cnt; _i++) {{
                if (_i) _s += ", ";
                size_t _z = 0;
                char* _e = temporal_as_hexwkb((const Temporal*) arr[_i], 0x04 /* WKB_EXTENDED */, &_z);
                if (_e) {{ _s += _e; free(_e); }}
                free(arr[_i]);
            }}
            _s += "}}";
            free(arr);
            return strdup(_s.c_str());""",
    """\
            Temporal* _e = (Temporal*) {seq_accessor_call};
            MEOS::Meos::freeTemporalObject(temp);
            if (!_e) {{
                return (char*)nullptr;
            }}
            size_t _z = 0;
            char* _hex = temporal_as_hexwkb((const Temporal*) _e, 0x04 /* WKB_EXTENDED */, &_z);
            free(_e);
            if (!_hex) {{
                return (char*)nullptr;
            }}
            return _hex;""",
    "tpose single-sequence accessor (one Temporal* -> EWKB hex)")

PHYSICAL_CPP_TPOSE_SEQ1 = _swap_once(
    PHYSICAL_CPP_TPOSE_SEQ_ARRAY,
    _FINALIZE_SEQARRAY_TPOSE, _FINALIZE_SEQ1_TPOSE, "tpose-seq1 finalize tail")

# ===========================================================================
# tpose parameterized transform (fold "tposetransform"): combine the 4-data-field
# tpose windowed builder with the constant-arg mechanism. The window is collected
# as a "[Pose(Point(x y),theta)@ts, ...]" continuous sequence, parsed via tpose_in
# and converted to a trgeometry via geo_tpose_to_trgeometry(unit-square refGeom,
# poseSeq) (same path as the tpose seqarray form), then a MEOS transform that takes
# the resulting Temporal* PLUS one or more CONSTANT (non-field) args is applied and
# the single Temporal* result is emitted as hex-WKB. Constants travel as literal
# strings (parser constantBuilder -> logical constArgs -> serde literals -> physical
# ctor) exactly as the tgeotransform fold; they are parsed to their C value/object
# only inside the finalize lambda. Derived by combining _FINALIZE_SEQARRAY_TPOSE's
# temp-construction with _FINALIZE_WKB_TGEO_CONST's const-parse + single-hex tail.
#
# The (Temporal*) cast on the MEOS-fn result is kept on purpose: trgeometry_sequence_n
# returns a TSequence* (a subtype of Temporal*), so the cast normalizes the result to
# the Temporal* the hex serializer + freeTemporalObject path expects.
# ===========================================================================

# tpose construction block lifted from _FINALIZE_SEQARRAY_TPOSE (the parseTemporalPoint
# block of _FINALIZE_WKB_TGEO_CONST swapped for tpose_in + geo_tpose_to_trgeometry).
# Note: _FINALIZE_WKB_TGEO_CONST frees trajStr BEFORE building temp; tpose construction
# also frees trajStr before building temp, so the post-free `if (!temp)` returns nullptr.
_FINALIZE_WKB_TPOSE_CONST = _swap_once(
    _FINALIZE_WKB_TGEO_CONST,
    """\
            std::string trajString(trajStr);
            void* temp = MEOS::Meos::parseTemporalPoint(trajString);
            free((void*)trajStr);
            if (!temp) {{
                return (char*)nullptr;
            }}""",
    """\
            std::string trajString(trajStr);
            Temporal* _poseSeq = tpose_in(trajString.c_str());
            free((void*)trajStr);
            if (!_poseSeq) {{
                return (char*)nullptr;
            }}
            GSERIALIZED* _refGeom = geom_in("Polygon((0 0,1 0,1 1,0 1,0 0))", -1);
            if (!_refGeom) {{
                free(_poseSeq);
                return (char*)nullptr;
            }}
            void* temp = geo_tpose_to_trgeometry(_refGeom, _poseSeq);
            free(_refGeom);
            free(_poseSeq);
            if (!temp) {{
                return (char*)nullptr;
            }}""",
    "tpose-const temp construction (tpose_in + geo_tpose_to_trgeometry)")

# Physical .cpp: from the base tpose template — empty-window scalar -> box (VARSIZED),
# instant-set braces -> sequence brackets, ctor gains the constArgs param + init, and
# the scalar finalize tail is replaced by _FINALIZE_WKB_TPOSE_CONST. Mirrors the
# PHYSICAL_CPP_TGEO_CONST derivation. The base tpose ctor's tail param/init differ from
# the tgeo one (4 PhysicalFunctions), so use tpose-specific anchors.
_CTOR_PARAM_TPOSE = """\
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef)"""
_CTOR_PARAM_TPOSE_CONST = """\
    PhysicalFunction timestampFunctionParam,
    Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
    std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef,
    std::vector<std::string> constArgsParam)"""

_CTOR_INIT_TPOSE = """\
    , timestampFunction(std::move(timestampFunctionParam))
{{
}}"""
_CTOR_INIT_TPOSE_CONST = """\
    , timestampFunction(std::move(timestampFunctionParam))
    , constArgs(std::move(constArgsParam))
{{
}}"""

PHYSICAL_CPP_TPOSE_CONST = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(
                _swap_once(
                    _swap_once(PHYSICAL_CPP_TPOSE, _EMPTY_SCALAR, _EMPTY_BOX, "tpose-const empty-window block"),
                    '            strcpy(buffer, "{{");', '            strcpy(buffer, "[");', "tpose-const open bracket -> sequence"),
                '            strcat(buffer, "}}");', '            strcat(buffer, "]");', "tpose-const close bracket -> sequence"),
            _CTOR_PARAM_TPOSE, _CTOR_PARAM_TPOSE_CONST, "tpose-const ctor param"),
        _CTOR_INIT_TPOSE, _CTOR_INIT_TPOSE_CONST, "tpose-const ctor init"),
    _FINALIZE_SCALAR_TGEO, _FINALIZE_WKB_TPOSE_CONST, "tpose-const finalize tail")

# Physical .hpp — append the constArgs ctor param + member to the base tpose .hpp.
_HPP_CTOR_TPOSE = """\
        PhysicalFunction timestampFunctionParam,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef);"""
_HPP_CTOR_TPOSE_CONST = """\
        PhysicalFunction timestampFunctionParam,
        Nautilus::Record::RecordFieldIdentifier resultFieldIdentifier,
        std::shared_ptr<Nautilus::Interface::BufferRef::TupleBufferRef> bufferRef,
        std::vector<std::string> constArgsParam);"""
_HPP_MEMBER_TPOSE = """\
    PhysicalFunction timestampFunction;
}};"""
_HPP_MEMBER_TPOSE_CONST = """\
    PhysicalFunction timestampFunction;
    std::vector<std::string> constArgs;
}};"""

PHYSICAL_HPP_TPOSE_CONST = _swap_once(
    _swap_once(
        _swap_once(PHYSICAL_HPP_TPOSE, "#include <cstddef>", "#include <cstddef>\n#include <string>\n#include <vector>",
                   "tpose-const hpp includes"),
        _HPP_CTOR_TPOSE, _HPP_CTOR_TPOSE_CONST, "tpose-const hpp ctor param"),
    _HPP_MEMBER_TPOSE, _HPP_MEMBER_TPOSE_CONST, "tpose-const hpp member")

# ===========================================================================
# Logical const variant for tpose (fold "tposetransform"): the 4-field tpose
# logical op additionally stores the constant literal strings, packs them via
# serializeConstArgs(), and the registrar reconstructs them from arguments.literals.
# Derived from the base tpose logical templates by surgical swaps.
# ===========================================================================

# --- HPP swaps ---
_LHPP_CREATE_TPOSE = """\
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& xField, const FieldAccessLogicalFunction& yField, const FieldAccessLogicalFunction& thetaField, const FieldAccessLogicalFunction& timestampField);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& xField,
        const FieldAccessLogicalFunction& yField,
        const FieldAccessLogicalFunction& thetaField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField);"""
_LHPP_CREATE_TPOSE_CONST = """\
    static std::shared_ptr<WindowAggregationLogicalFunction>
    create(const FieldAccessLogicalFunction& xField, const FieldAccessLogicalFunction& yField, const FieldAccessLogicalFunction& thetaField, const FieldAccessLogicalFunction& timestampField, std::vector<std::string> constArgs);

    {nebula_name}AggregationLogicalFunction(
        const FieldAccessLogicalFunction& xField,
        const FieldAccessLogicalFunction& yField,
        const FieldAccessLogicalFunction& thetaField,
        const FieldAccessLogicalFunction& timestampField,
        const FieldAccessLogicalFunction& asField,
        std::vector<std::string> constArgs);

    [[nodiscard]] const std::vector<std::string>& getConstArgs() const noexcept {{ return constArgs; }}"""
_LHPP_MEMBER_TPOSE = """\
    FieldAccessLogicalFunction xField;
    FieldAccessLogicalFunction yField;
    FieldAccessLogicalFunction thetaField;
    FieldAccessLogicalFunction timestampField;
}};"""
_LHPP_MEMBER_TPOSE_CONST = """\
    FieldAccessLogicalFunction xField;
    FieldAccessLogicalFunction yField;
    FieldAccessLogicalFunction thetaField;
    FieldAccessLogicalFunction timestampField;
    std::vector<std::string> constArgs;
}};"""

LOGICAL_HPP_TPOSE_CONST = _swap_once(
    _swap_once(
        _swap_once(LOGICAL_HPP_TPOSE,
                   "#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>",
                   "#include <string>\n#include <vector>\n#include <Operators/Windows/Aggregations/WindowAggregationLogicalFunction.hpp>",
                   "tpose-const lhpp includes"),
        _LHPP_CREATE_TPOSE, _LHPP_CREATE_TPOSE_CONST, "tpose-const lhpp create/ctor"),
    _LHPP_MEMBER_TPOSE, _LHPP_MEMBER_TPOSE_CONST, "tpose-const lhpp member")

# --- CPP swaps ---
_LCPP_INCLUDES_TPOSE = "#include <memory>\n#include <string>\n#include <string_view>"
_LCPP_INCLUDES_TPOSE_CONST = "#include <memory>\n#include <string>\n#include <string_view>\n#include <utility>\n#include <vector>"

_LCPP_CTOR_TPOSE = """\
{nebula_name}AggregationLogicalFunction::{nebula_name}AggregationLogicalFunction(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField)
    : WindowAggregationLogicalFunction(
          xField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          xField,
          asField)
    , xField(xField)
    , yField(yField)
    , thetaField(thetaField)
    , timestampField(timestampField)
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(xField, yField, thetaField, timestampField, xField);
}}"""
_LCPP_CTOR_TPOSE_CONST = """\
{nebula_name}AggregationLogicalFunction::{nebula_name}AggregationLogicalFunction(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField,
    const FieldAccessLogicalFunction& asField,
    std::vector<std::string> constArgs)
    : WindowAggregationLogicalFunction(
          xField.getDataType(),
          DataTypeProvider::provideDataType(partialAggregateStampType),
          DataTypeProvider::provideDataType(finalAggregateStampType),
          xField,
          asField)
    , xField(xField)
    , yField(yField)
    , thetaField(thetaField)
    , timestampField(timestampField)
    , constArgs(std::move(constArgs))
{{
}}

std::shared_ptr<WindowAggregationLogicalFunction>
{nebula_name}AggregationLogicalFunction::create(
    const FieldAccessLogicalFunction& xField,
    const FieldAccessLogicalFunction& yField,
    const FieldAccessLogicalFunction& thetaField,
    const FieldAccessLogicalFunction& timestampField,
    std::vector<std::string> constArgs)
{{
    return std::make_shared<{nebula_name}AggregationLogicalFunction>(xField, yField, thetaField, timestampField, xField, std::move(constArgs));
}}"""

_LCPP_SERIALIZE_TPOSE = """\
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(xField, yField, thetaField, timestampField, asField);
    saf.set_type(std::string(NAME));
    return saf;"""
_LCPP_SERIALIZE_TPOSE_CONST = """\
    auto saf = TemporalAggregationSerde::serializeTemporalSequence(xField, yField, thetaField, timestampField, asField);
    TemporalAggregationSerde::serializeConstArgs(saf, constArgs);
    saf.set_type(std::string(NAME));
    return saf;"""

_LCPP_REGISTRAR_TPOSE = """\
    if (arguments.fields.size() == 5)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4]);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires x, y, theta, timestamp, and alias fields but got {{}}",
        arguments.fields.size());"""
_LCPP_REGISTRAR_TPOSE_CONST = """\
    if (arguments.fields.size() == 5)
    {{
        auto ptr = std::make_shared<{nebula_name}AggregationLogicalFunction>(
            arguments.fields[0], arguments.fields[1], arguments.fields[2], arguments.fields[3], arguments.fields[4],
            arguments.literals);
        return ptr;
    }}
    throw CannotDeserialize(
        "{nebula_name}AggregationLogicalFunction requires x, y, theta, timestamp, and alias fields but got {{}}",
        arguments.fields.size());"""

LOGICAL_CPP_TPOSE_CONST = _swap_once(
    _swap_once(
        _swap_once(
            _swap_once(LOGICAL_CPP_TPOSE, _LCPP_INCLUDES_TPOSE, _LCPP_INCLUDES_TPOSE_CONST, "tpose-const lcpp includes"),
            _LCPP_CTOR_TPOSE, _LCPP_CTOR_TPOSE_CONST, "tpose-const lcpp ctor+create"),
        _LCPP_SERIALIZE_TPOSE, _LCPP_SERIALIZE_TPOSE_CONST, "tpose-const lcpp serialize"),
    _LCPP_REGISTRAR_TPOSE, _LCPP_REGISTRAR_TPOSE_CONST, "tpose-const lcpp registrar")

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
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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
#include <meos_cbuffer.h>
#include <meos_npoint.h>
#include <meos_pose.h>
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
# Array-make aggregate template (windowed value-collect -> {make_fn}(arr, n)).
#
# Unlike SCALARFOLD/SETFOLD (which fold each event through a MEOS transition fn
# into an opaque state), this COLLECTS the window's scalar values into a malloc'd
# C array (size = numberOfEntries, known at lower()) and calls the MEOS array
# constructor `{make_fn}(({elem_cpp}*) arr, n)` ONCE at finalize, serializing the
# resulting Set via the external typed wrapper `{set_out_call}`. Same proven
# cross-invoke pointer-threading as CrossDistanceAggregation's scratch buffer.
# Derived from SCALARFOLD by swapping ONLY the fold+serialize region of lower();
# lift / combine / reset / cleanup / ctor stay byte-identical.
# ===========================================================================
_SCALARFOLD_FOLD_AND_SERIALIZE = """\
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
        spanState);"""

_ARRAYMAKE_FOLD_AND_SERIALIZE = """\
    // Collect the windowed scalar values into a C array, then build the result
    // via the MEOS array constructor {make_fn}. The array pointer threads across
    // events as an opaque void* (Nautilus invoke ABI), sized to numberOfEntries.
    auto arrState = nautilus::invoke(
        +[](size_t n) -> void* {{ return malloc(sizeof({elem_cpp}) * (n > 0 ? n : 1)); }},
        numberOfEntries);

    auto arrIdx = nautilus::val<int64_t>(0);
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        auto value = valueRaw.cast<nautilus::val<{fold_field_cpp_type}>>();

        nautilus::invoke(
            +[](void* a, int64_t i, {fold_field_cpp_type} val) -> void
            {{ (({elem_cpp}*) a)[i] = ({elem_cpp}) val; }},
            arrState,
            arrIdx,
            value);
        arrIdx = arrIdx + nautilus::val<int64_t>(1);
    }}

    auto boxStr = nautilus::invoke(
        +[](void* a, int64_t n) -> char*
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            {make_ret_type}* s = {make_fn}(({elem_cpp}*) a, (int) n);
            free(a);
            if (!s) {{
                return (char*)nullptr;
            }}
            char* out = {set_out_call};
            free(s);
            return out;
        }},
        arrState,
        arrIdx);"""

PHYSICAL_CPP_ARRAYMAKE = _swap_once(
    PHYSICAL_CPP_SCALARFOLD, _SCALARFOLD_FOLD_AND_SERIALIZE, _ARRAYMAKE_FOLD_AND_SERIALIZE,
    "scalarfold fold+serialize -> arraymake (collect C array + *_make)")

# Object-set array constructor (fold "objarraymake"): the windowed VALUES are
# VARSIZED object literals (text / Cbuffer / Npoint / Pose / geometry), not
# numbers. Parse each event's literal via {elem_in} into a heap {elem_cpp}*,
# collect the pointers into a {elem_cpp}*[] array, build the set via {make_fn}
# ({make_arg_type}), then free each element and the array. Derived from the
# numeric arraymake by swapping only the collect/serialize body; the value field
# is VARSIZED so the descriptor relaxes value_type_guard.
_OBJ_ARRAYMAKE_FOLD_AND_SERIALIZE = """\
    // Collect the windowed object literals: parse each event's VARSIZED value
    // via {elem_in} into a heap pointer, store it in a {elem_cpp}*[] array, then
    // build the result set via {make_fn} and free each element + the array.
    auto arrState = nautilus::invoke(
        +[](size_t n) -> void* {{ return malloc(sizeof({elem_cpp}*) * (n > 0 ? n : 1)); }},
        numberOfEntries);

    auto arrIdx = nautilus::val<int64_t>(0);
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        auto valVar = valueRaw.cast<VariableSizedData>();

        arrIdx = nautilus::invoke(
            +[](void* a, int64_t i, const char* vp, uint32_t vs) -> int64_t
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                std::string s(vp, vs);
                while (!s.empty() && (s.front()=='\\'' || s.front()=='"')) s.erase(s.begin());
                while (!s.empty() && (s.back()=='\\'' || s.back()=='"')) s.pop_back();
                {elem_cpp}* e = {elem_in}(s.c_str(){elem_in_extra});
                if (e) {{ (({elem_cpp}**) a)[i] = e; return i + 1; }}
                return i;
            }},
            arrState,
            arrIdx,
            valVar.getContent(),
            valVar.getContentSize());
    }}

    auto boxStr = nautilus::invoke(
        +[](void* a, int64_t n) -> char*
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            if (n <= 0) {{ free(a); return (char*)nullptr; }}
            {make_ret_type}* s = {make_fn}(({make_arg_type}) a, (int) n);
            for (int64_t i = 0; i < n; i++) {{ free((({elem_cpp}**) a)[i]); }}
            free(a);
            if (!s) {{ return (char*)nullptr; }}
            char* out = {set_out_call};
            free(s);
            return out;
        }},
        arrState,
        arrIdx);"""

PHYSICAL_CPP_OBJ_ARRAYMAKE = _swap_once(
    PHYSICAL_CPP_ARRAYMAKE, _ARRAYMAKE_FOLD_AND_SERIALIZE, _OBJ_ARRAYMAKE_FOLD_AND_SERIALIZE,
    "arraymake numeric collect -> object-pointer collect (*_in parse + *set_make)")

# ---------------------------------------------------------------------------
# objarraymake variants that build a *Temporal* (not a Set) and serialize it as
# hex-WKB. Two flavours share the same collect loop (a {elem_cpp}*[] pointer
# array parsed via {elem_in}) and differ only in the make call:
#   * "tempmergearray": make takes (arr, count) only        (temporal_merge_array)
#   * "tseqsetmakegaps": make takes (arr, count) + the FIXED trailing args baked
#     into the finalize via {make_extra_args} (interp, maxt interval, maxdist)
# The serialize tail is the proven temporal_as_hexwkb(res, 0x04, &sz) form, not a
# *_out, because the result is a Temporal*/TSequenceSet* (a temporal value).
# Only the make+serialize lambda is swapped; the collect loop is reused verbatim.
_OBJ_ARRAYMAKE_MAKE_LINE = """\
            {make_ret_type}* s = {make_fn}(({make_arg_type}) a, (int) n);
            for (int64_t i = 0; i < n; i++) {{ free((({elem_cpp}**) a)[i]); }}
            free(a);
            if (!s) {{ return (char*)nullptr; }}
            char* out = {set_out_call};
            free(s);
            return out;"""

_TEMPMERGEARRAY_MAKE_LINE = """\
            Temporal* s = {make_fn}(({make_arg_type}) a, (int) n);
            for (int64_t i = 0; i < n; i++) {{ free((({elem_cpp}**) a)[i]); }}
            free(a);
            if (!s) {{ return (char*)nullptr; }}
            size_t hexSize = 0;
            char* out = temporal_as_hexwkb(s, 0x04 /* WKB_EXTENDED */, &hexSize);
            free(s);
            return out;"""

_TSEQSETMAKEGAPS_MAKE_LINE = """\
            Interval* maxt = interval_in("{gaps_maxt}", -1);
            Temporal* s = (Temporal*) {make_fn}(({make_arg_type}) a, (int) n, {gaps_interp}, maxt, {gaps_maxdist});
            for (int64_t i = 0; i < n; i++) {{ free((({elem_cpp}**) a)[i]); }}
            free(a);
            free(maxt);
            if (!s) {{ return (char*)nullptr; }}
            size_t hexSize = 0;
            char* out = temporal_as_hexwkb(s, 0x04 /* WKB_EXTENDED */, &hexSize);
            free(s);
            return out;"""

PHYSICAL_CPP_TEMPMERGEARRAY = _swap_once(
    PHYSICAL_CPP_OBJ_ARRAYMAKE, _OBJ_ARRAYMAKE_MAKE_LINE, _TEMPMERGEARRAY_MAKE_LINE,
    "objarraymake set tail -> temporal_merge_array hex tail")

PHYSICAL_CPP_TSEQSETMAKEGAPS = _swap_once(
    PHYSICAL_CPP_OBJ_ARRAYMAKE, _OBJ_ARRAYMAKE_MAKE_LINE, _TSEQSETMAKEGAPS_MAKE_LINE,
    "objarraymake set tail -> tsequenceset_make_gaps hex tail (fixed trailing args)")

# ---------------------------------------------------------------------------
# By-value array constructor (fold "spansetmake"): unlike objarraymake (a pointer
# array), {make_fn} expects a CONTIGUOUS {elem_cpp} value array. Parse each
# event's VARSIZED span literal via {elem_in} into a heap {elem_cpp}*, memcpy the
# value into arr[i], and free the parsed heap span immediately (the array owns a
# copy). At finalize call {make_fn}((({elem_cpp}*) a), count) and serialize via
# {set_out_call}; there is NO per-element free loop (values are inline) — only the
# array and the result are freed. Derived from objarraymake by swapping the array
# allocation, the per-event store, and the make+serialize lambda.
_SPANSETMAKE_FOLD_AND_SERIALIZE = """\
    // Collect the windowed span literals into a CONTIGUOUS {elem_cpp} value array:
    // parse each via {elem_in} into a heap {elem_cpp}*, memcpy it into arr[i], and
    // free the parsed heap span. At finalize build the result via {make_fn} over
    // the value array, serialize via {set_out_call}, then free the array + result.
    auto arrState = nautilus::invoke(
        +[](size_t n) -> void* {{ return malloc(sizeof({elem_cpp}) * (n > 0 ? n : 1)); }},
        numberOfEntries);

    auto arrIdx = nautilus::val<int64_t>(0);
    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        auto valVar = valueRaw.cast<VariableSizedData>();

        arrIdx = nautilus::invoke(
            +[](void* a, int64_t i, const char* vp, uint32_t vs) -> int64_t
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                std::string s(vp, vs);
                while (!s.empty() && (s.front()=='\\'' || s.front()=='"')) s.erase(s.begin());
                while (!s.empty() && (s.back()=='\\'' || s.back()=='"')) s.pop_back();
                {elem_cpp}* e = {elem_in}(s.c_str(){elem_in_extra});
                if (e) {{ (({elem_cpp}*) a)[i] = *e; free(e); return i + 1; }}
                return i;
            }},
            arrState,
            arrIdx,
            valVar.getContent(),
            valVar.getContentSize());
    }}

    auto boxStr = nautilus::invoke(
        +[](void* a, int64_t n) -> char*
        {{
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            if (n <= 0) {{ free(a); return (char*)nullptr; }}
            {make_ret_type}* s = {make_fn}(({elem_cpp}*) a, (int) n);
            free(a);
            if (!s) {{ return (char*)nullptr; }}
            char* out = {set_out_call};
            free(s);
            return out;
        }},
        arrState,
        arrIdx);"""

PHYSICAL_CPP_SPANSETMAKE = _swap_once(
    PHYSICAL_CPP_OBJ_ARRAYMAKE, _OBJ_ARRAYMAKE_FOLD_AND_SERIALIZE, _SPANSETMAKE_FOLD_AND_SERIALIZE,
    "objarraymake pointer-array collect -> contiguous value-array collect (spanset_make)")

# ===========================================================================
# Temporal-aggregate template (windowed tMin/tMax/tSum/tAnd/tOr/tAvg -> Temporal,
# serialized as hex-WKB). Reuses the scalar-fold PagedVector scaffold (lift =
# buffer raw (value, ts); combine = concat raw events), but lower() folds EACH
# event through the per-op MEOS `*_tagg`/`*_tavg` transition fn into a SkipList
# (so same-timestamp events are merged by the transfn's aggregation function),
# then materializes the aggregate Temporal via the finalfn and emits its
# hex-WKB. Because the SkipList is built fresh in lower() from the complete raw
# event multiset, combine stays correct without a per-op SkipList combinefn.
# Derived from PHYSICAL_CPP_SCALARFOLD by swapping only the lower() fold/serialize
# block (lift / combine / reset / cleanup stay byte-identical).
# ===========================================================================
_SCALARFOLD_LOWER_FOLD = """\
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
        spanState);"""

_TAGG_LOWER_FOLD = """\
    // Fold each windowed event through the per-op MEOS tagg transition fn into a
    // SkipList state (NULL initial -> transfn allocates). Same-timestamp events
    // are merged by the transition fn's aggregation function (sum/min/max/...).
    auto skipState = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector*) -> void* {{ return nullptr; }},
        pagedVectorPtr);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        const auto timestampRaw = itemRecord.read(std::string(TimestampFieldName));
        auto value = valueRaw.cast<nautilus::val<{lift_value_cpp_type}>>();
        auto timestamp = timestampRaw.cast<nautilus::val<int64_t>>();

        skipState = nautilus::invoke(
            +[](void* state, {lift_value_cpp_type} valueVal, int64_t tsVal) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                long long adjustedTime = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
                std::string tsS = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                char itemStr[80];
                sprintf(itemStr, "{value_printf_fmt}@%s", {value_expr}, tsS.c_str());
                Temporal* inst = {tnumber_in_fn}(itemStr);
                if (!inst) {{ return state; }}
                {tagg_pre_call}SkipList* ns = {tagg_transfn}(static_cast<SkipList*>(state), inst{tagg_extra_arg});
                {tagg_post_call}free(inst);
                return reinterpret_cast<void*>(ns);
            }},
            skipState,
            value,
            timestamp);
    }}

    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{
                return (char*)nullptr;
            }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal* res = {tagg_finalfn}(static_cast<SkipList*>(state));
            if (!res) {{
                return (char*)nullptr;
            }}
            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &hexSize);
            free(res);
            return hexOut;
        }},
        skipState);"""

PHYSICAL_CPP_TAGG_WKB = _swap_once(
    PHYSICAL_CPP_SCALARFOLD, _SCALARFOLD_LOWER_FOLD, _TAGG_LOWER_FOLD,
    "scalarfold lower -> tagg skiplist lower (finalfn + hex-WKB)")

# ttext temporal-aggregate (windowed ttext_tmin/tmax -> Temporal text, hex-WKB).
# The value field is VARSIZED (text), not numeric, so read it as bytes and build
# a "text"@ts instant (std::string concat, not sprintf). Same SkipList fold +
# finalfn + hex-WKB tail as TAGG_WKB.
_TAGG_NUMERIC_READFOLD = '''\
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        const auto timestampRaw = itemRecord.read(std::string(TimestampFieldName));
        auto value = valueRaw.cast<nautilus::val<{lift_value_cpp_type}>>();
        auto timestamp = timestampRaw.cast<nautilus::val<int64_t>>();

        skipState = nautilus::invoke(
            +[](void* state, {lift_value_cpp_type} valueVal, int64_t tsVal) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                long long adjustedTime = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
                std::string tsS = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                char itemStr[80];
                sprintf(itemStr, "{value_printf_fmt}@%s", {value_expr}, tsS.c_str());
                Temporal* inst = {tnumber_in_fn}(itemStr);
                if (!inst) {{ return state; }}
                {tagg_pre_call}SkipList* ns = {tagg_transfn}(static_cast<SkipList*>(state), inst{tagg_extra_arg});
                {tagg_post_call}free(inst);
                return reinterpret_cast<void*>(ns);
            }},
            skipState,
            value,
            timestamp);'''

_TAGG_TEXT_READFOLD = '''\
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        const auto timestampRaw = itemRecord.read(std::string(TimestampFieldName));
        auto valVar = valueRaw.cast<VariableSizedData>();
        auto timestamp = timestampRaw.cast<nautilus::val<int64_t>>();

        skipState = nautilus::invoke(
            +[](void* state, const char* valPtr, uint32_t valSize, int64_t tsVal) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                long long adjustedTime = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
                std::string tsS = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                std::string txt(valPtr, valSize);
                while (!txt.empty() && (txt.front()=='\\'' || txt.front()=='"')) txt.erase(txt.begin());
                while (!txt.empty() && (txt.back()=='\\'' || txt.back()=='"')) txt.pop_back();
                std::string wkt = std::string("\\"") + txt + "\\"@" + tsS;
                Temporal* inst = {tnumber_in_fn}(wkt.c_str());
                if (!inst) {{ return state; }}
                SkipList* ns = {tagg_transfn}(static_cast<SkipList*>(state), inst);
                free(inst);
                return reinterpret_cast<void*>(ns);
            }},
            skipState,
            valVar.getContent(),
            valVar.getContentSize(),
            timestamp);'''

PHYSICAL_CPP_TTEXT_TAGG_WKB = _swap_once(
    PHYSICAL_CPP_TAGG_WKB, _TAGG_NUMERIC_READFOLD, _TAGG_TEXT_READFOLD,
    "tagg numeric read/fold -> ttext varsized read/fold")

# ===========================================================================
# Container-fold aggregate (windowed *_extent / *_union over a stream of VARSIZED
# container literals: Span/Set/SpanSet/object -> Span/Set/SpanSet, serialized as
# text). Reuses the scalar-fold scaffold (the timestamp is windowing-only, not
# part of the aggregate, so there is NO ts in the fold). Per-op heterogeneity
# (parser, state type, finalfn, *_out serializer) rides in two body placeholders.
# ===========================================================================
_CONTAINER_LOWER_FOLD = '''\
    // Fold each windowed VARSIZED container literal through the MEOS transfn into
    // a state pointer (Span/SpanSet/Set). Raw-buffer replay keeps combine correct.
    auto cstate = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector*) -> void* {{ return nullptr; }},
        pagedVectorPtr);

    const auto endIt = pagedVectorRef.end(allFieldNames);
    for (auto candidateIt = pagedVectorRef.begin(allFieldNames); candidateIt != endIt; ++candidateIt)
    {{
        const auto itemRecord = *candidateIt;
        const auto valueRaw = itemRecord.read(std::string(ValueFieldName));
        auto valVar = valueRaw.cast<VariableSizedData>();

        cstate = nautilus::invoke(
            +[](void* state, const char* valPtr, uint32_t valSize) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                std::string s(valPtr, valSize);
                while (!s.empty() && (s.front()=='\\'' || s.front()=='"')) s.erase(s.begin());
                while (!s.empty() && (s.back()=='\\'' || s.back()=='"')) s.pop_back();
                {container_fold_body}
            }},
            cstate,
            valVar.getContent(),
            valVar.getContentSize());
    }}

    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{ return (char*)nullptr; }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            {container_serialize_body}
        }},
        cstate);'''

PHYSICAL_CPP_CONTAINER_FOLD = _swap_once(
    PHYSICAL_CPP_SCALARFOLD, _SCALARFOLD_LOWER_FOLD, _CONTAINER_LOWER_FOLD,
    "scalarfold lower -> container varsized fold (parse + transfn + finalfn/out)")

# ===========================================================================
# tgeo temporal-aggregate (windowed tpoint_tcentroid -> Temporal point, hex-WKB).
# Reuses the tgeo (lon, lat, ts) scaffold but swaps the trajectory-assemble
# lower() for a per-event SkipList fold (same-timestamp points averaged by the
# centroid transfn), finalfn -> temporal_as_hexwkb. Same raw-buffer-replay
# correctness as TAGG_WKB.
# ===========================================================================
_TGEO_LOWER_ASSEMBLE = '''\
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
    return resultRecord;'''

_TGEO_TAGG_LOWER = '''\
    // Fold each windowed point through the per-op centroid transfn into a
    // SkipList (same-timestamp points averaged), then materialize via the
    // finalfn and emit hex-WKB. Raw-buffer replay keeps combine correct.
    auto skipState = nautilus::invoke(
        +[](const Nautilus::Interface::PagedVector*) -> void* {{ return nullptr; }},
        pagedVectorPtr);

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

        skipState = nautilus::invoke(
            +[](void* state, double lonVal, double latVal, int64_t tsVal) -> void*
            {{
                MEOS::Meos::ensureMeosInitialized();
                std::lock_guard<std::mutex> lock({mutex_name});
                long long adjustedTime = (tsVal > 1000000000000LL) ? (tsVal / 1000) : tsVal;
                std::string tsS = MEOS::Meos::convertSecondsToTimestamp(adjustedTime);
                char itemStr[120];
                sprintf(itemStr, "Point(%.6f %.6f)@%s", lonVal, latVal, tsS.c_str());
                Temporal* inst = tgeompoint_in(itemStr);
                if (!inst) {{ return state; }}
                SkipList* ns = {tagg_transfn}(static_cast<SkipList*>(state), inst);
                free(inst);
                return reinterpret_cast<void*>(ns);
            }},
            skipState, lon, lat, timestamp);
    }}

    auto boxStr = nautilus::invoke(
        +[](void* state) -> char*
        {{
            if (!state) {{ return (char*)nullptr; }}
            MEOS::Meos::ensureMeosInitialized();
            std::lock_guard<std::mutex> lock({mutex_name});
            Temporal* res = {tagg_finalfn}(static_cast<SkipList*>(state));
            if (!res) {{ return (char*)nullptr; }}
            size_t hexSize = 0;
            char* hexOut = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &hexSize);
            free(res);
            return hexOut;
        }},
        skipState);

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
    return resultRecord;'''

# The tgeo scaffold's empty-window block returns nautilus::val<{return_cpp_type}>(0)
# (a scalar); the tagg variant emits VARSIZED, so the empty case must write an
# empty VariableSizedData instead.
_TGEO_EMPTY_SCALAR = '''\
    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, nautilus::val<{return_cpp_type}>(0));
        return resultRecord;
    }}'''
_TGEO_EMPTY_VARSIZED = '''\
    if (numberOfEntries == nautilus::val<size_t>(0)) {{
        auto emptyVarSized = pipelineMemoryProvider.arena.allocateVariableSizedData(0);
        Nautilus::Record resultRecord;
        resultRecord.write(resultFieldIdentifier, emptyVarSized);
        return resultRecord;
    }}'''

PHYSICAL_CPP_TGEO_TAGG_WKB = _swap_once(
    _swap_once(PHYSICAL_CPP_TGEO, _TGEO_EMPTY_SCALAR, _TGEO_EMPTY_VARSIZED,
               "tgeo empty-window scalar -> varsized"),
    _TGEO_LOWER_ASSEMBLE, _TGEO_TAGG_LOWER,
    "tgeo trajectory-assemble lower -> tgeo tagg skiplist lower (finalfn + hex-WKB)")

# ===========================================================================
# Parser-glue templates: TWO dispatch sites in AntlrSQLQueryPlanCreator.cpp.
# Site 1 is the dedicated-token case-switch (~line 965 in mariana's tree).
# Site 2 is the IDENTIFIER fallback `else if (funcName == "TOKEN")` chain
# (~line 2062 in mariana's tree).
# ===========================================================================

# Site 1 — case-switch dispatch. Two shapes (tgeo 3-arg, tnumber 2-arg).
CASE_SWITCH_TGEO = """\
        /* BEGIN CODEGEN GLUE: {sql_token} (case-switch) */
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
        /* END CODEGEN GLUE: {sql_token} (case-switch) */
"""

CASE_SWITCH_TNUMBER = """\
        /* BEGIN CODEGEN GLUE: {sql_token} (case-switch) */
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
        /* END CODEGEN GLUE: {sql_token} (case-switch) */
"""

# Site 2 — funcName == "TOKEN" string chain.
FUNCNAME_CHAIN_TGEO = """\
            /* BEGIN CODEGEN GLUE: {sql_token} (funcName chain) */
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
            /* END CODEGEN GLUE: {sql_token} (funcName chain) */
"""

FUNCNAME_CHAIN_TNUMBER = """\
            /* BEGIN CODEGEN GLUE: {sql_token} (funcName chain) */
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
            /* END CODEGEN GLUE: {sql_token} (funcName chain) */
"""

# Site 3 — optimizer logical→physical lowering rule.
OPTIMIZER_LOWERING_TGEO = """\
        /* BEGIN CODEGEN GLUE: {class_name_token} (optimizer lowering) */
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
        /* END CODEGEN GLUE: {class_name_token} (optimizer lowering) */
"""

OPTIMIZER_LOWERING_TNUMBER = """\
        /* BEGIN CODEGEN GLUE: {class_name_token} (optimizer lowering) */
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
        /* END CODEGEN GLUE: {class_name_token} (optimizer lowering) */
"""

# tpose parser/optimizer glue — FOUR field args (x, y, theta, ts).
CASE_SWITCH_TPOSE = """\
        /* BEGIN CODEGEN GLUE: {sql_token} (case-switch) */
        case AntlrSQLLexer::{sql_token}:
            // {comment_one_liner}
            if (helpers.top().functionBuilder.size() != 4) {{
                throw InvalidQuerySyntax("{sql_token} requires exactly four arguments (x, y, theta, timestamp), but got {{}}", helpers.top().functionBuilder.size());
            }}
            {{
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto thetaFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto yFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto xFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!xFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !yFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !thetaFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {{
                    throw InvalidQuerySyntax("{sql_token} arguments must be field references");
                }}

                helpers.top().windowAggs.push_back(
                    {nebula_name}AggregationLogicalFunction::create(xFunction.get<FieldAccessLogicalFunction>(),
                                                                    yFunction.get<FieldAccessLogicalFunction>(),
                                                                    thetaFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>()));
                helpers.top().functionBuilder.push_back(xFunction);
            }}
            break;
        /* END CODEGEN GLUE: {sql_token} (case-switch) */
"""

FUNCNAME_CHAIN_TPOSE = """\
            /* BEGIN CODEGEN GLUE: {sql_token} (funcName chain) */
            else if (funcName == "{sql_token}")
            {{
                if (helpers.top().functionBuilder.size() < 4)
                {{
                    throw InvalidQuerySyntax("{sql_token} requires four arguments at {{}}", context->getText());
                }}
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto theta = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto y = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto x = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back({nebula_name}AggregationLogicalFunction::create(x, y, theta, ts));
            }}
            /* END CODEGEN GLUE: {sql_token} (funcName chain) */
"""

OPTIMIZER_LOWERING_TPOSE = """\
        /* BEGIN CODEGEN GLUE: {class_name_token} (optimizer lowering) */
        if (name == std::string_view("{class_name_token}"))
        {{
            auto specificDescriptor = std::dynamic_pointer_cast<{nebula_name}AggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected {nebula_name}AggregationLogicalFunction for {class_name_token}");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<{nebula_name}AggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef);
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }}
        /* END CODEGEN GLUE: {class_name_token} (optimizer lowering) */
"""

# ===========================================================================
# Parameterized tgeo transform glue (fold "tgeotransform"): the SQL call is
# {sql_token}(lon, lat, ts, <const0>, <const1>, ...). The 3 field args land in
# functionBuilder; the trailing {num_const_args} literal constants land in
# constantBuilder (the parser parks literals there). Pop the constants (reverse
# order -> source order), then the 3 fields, and pass the const literal STRINGS
# straight to create(). Mirrors PAIR_MEETING / CROSS_DISTANCE.
# ===========================================================================
CASE_SWITCH_TGEO_CONST = """\
        /* BEGIN CODEGEN GLUE: {sql_token} (case-switch) */
        case AntlrSQLLexer::{sql_token}:
            // {comment_one_liner}
            {{
                if (helpers.top().constantBuilder.size() < {num_const_args}) {{
                    throw InvalidQuerySyntax("{sql_token} requires {num_const_args} constant argument(s) after lon, lat, timestamp, but got {{}}", helpers.top().constantBuilder.size());
                }}
                std::vector<std::string> constArgs({num_const_args});
                for (size_t i = 0; i < static_cast<size_t>({num_const_args}); ++i) {{
                    constArgs[static_cast<size_t>({num_const_args}) - 1 - i] = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                }}

                if (helpers.top().functionBuilder.size() != 3) {{
                    throw InvalidQuerySyntax("{sql_token} requires exactly three field arguments (longitude, latitude, timestamp), but got {{}}", helpers.top().functionBuilder.size());
                }}
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto latitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto longitudeFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!longitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !latitudeFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {{
                    throw InvalidQuerySyntax("{sql_token} field arguments must be field references");
                }}

                helpers.top().windowAggs.push_back(
                    {nebula_name}AggregationLogicalFunction::create(longitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    latitudeFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>(),
                                                                    std::move(constArgs)));
                helpers.top().functionBuilder.push_back(longitudeFunction);
            }}
            break;
        /* END CODEGEN GLUE: {sql_token} (case-switch) */
"""

FUNCNAME_CHAIN_TGEO_CONST = """\
            /* BEGIN CODEGEN GLUE: {sql_token} (funcName chain) */
            else if (funcName == "{sql_token}")
            {{
                if (helpers.top().constantBuilder.size() < {num_const_args})
                {{
                    throw InvalidQuerySyntax("{sql_token} requires {num_const_args} constant argument(s) at {{}}", context->getText());
                }}
                std::vector<std::string> constArgs({num_const_args});
                for (size_t i = 0; i < static_cast<size_t>({num_const_args}); ++i) {{
                    constArgs[static_cast<size_t>({num_const_args}) - 1 - i] = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                }}
                if (helpers.top().functionBuilder.size() < 3)
                {{
                    throw InvalidQuerySyntax("{sql_token} requires three field arguments at {{}}", context->getText());
                }}
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lat = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto lon = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back({nebula_name}AggregationLogicalFunction::create(lon, lat, ts, std::move(constArgs)));
            }}
            /* END CODEGEN GLUE: {sql_token} (funcName chain) */
"""

OPTIMIZER_LOWERING_TGEO_CONST = """\
        /* BEGIN CODEGEN GLUE: {class_name_token} (optimizer lowering) */
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
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }}
        /* END CODEGEN GLUE: {class_name_token} (optimizer lowering) */
"""

# ===========================================================================
# Parameterized tpose transform glue (fold "tposetransform"): the SQL call is
# {sql_token}(x, y, theta, ts, <const0>, <const1>, ...). The 4 field args land in
# functionBuilder; the trailing {num_const_args} literal constants land in
# constantBuilder. Pop the constants (reverse order -> source order), then the 4
# fields (ts, theta, y, x order — like CASE_SWITCH_TPOSE), and pass the const
# literal STRINGS straight to create(x, y, theta, ts, constArgs). Combines
# CASE_SWITCH_TGEO_CONST's constant-pop with CASE_SWITCH_TPOSE's 4-field pop.
# ===========================================================================
CASE_SWITCH_TPOSE_CONST = """\
        /* BEGIN CODEGEN GLUE: {sql_token} (case-switch) */
        case AntlrSQLLexer::{sql_token}:
            // {comment_one_liner}
            {{
                if (helpers.top().constantBuilder.size() < {num_const_args}) {{
                    throw InvalidQuerySyntax("{sql_token} requires {num_const_args} constant argument(s) after x, y, theta, timestamp, but got {{}}", helpers.top().constantBuilder.size());
                }}
                std::vector<std::string> constArgs({num_const_args});
                for (size_t i = 0; i < static_cast<size_t>({num_const_args}); ++i) {{
                    constArgs[static_cast<size_t>({num_const_args}) - 1 - i] = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                }}

                if (helpers.top().functionBuilder.size() != 4) {{
                    throw InvalidQuerySyntax("{sql_token} requires exactly four field arguments (x, y, theta, timestamp), but got {{}}", helpers.top().functionBuilder.size());
                }}
                const auto timestampFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto thetaFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto yFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();
                const auto xFunction = helpers.top().functionBuilder.back();
                helpers.top().functionBuilder.pop_back();

                if (!xFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !yFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !thetaFunction.tryGet<FieldAccessLogicalFunction>() ||
                    !timestampFunction.tryGet<FieldAccessLogicalFunction>()) {{
                    throw InvalidQuerySyntax("{sql_token} field arguments must be field references");
                }}

                helpers.top().windowAggs.push_back(
                    {nebula_name}AggregationLogicalFunction::create(xFunction.get<FieldAccessLogicalFunction>(),
                                                                    yFunction.get<FieldAccessLogicalFunction>(),
                                                                    thetaFunction.get<FieldAccessLogicalFunction>(),
                                                                    timestampFunction.get<FieldAccessLogicalFunction>(),
                                                                    std::move(constArgs)));
                helpers.top().functionBuilder.push_back(xFunction);
            }}
            break;
        /* END CODEGEN GLUE: {sql_token} (case-switch) */
"""

FUNCNAME_CHAIN_TPOSE_CONST = """\
            /* BEGIN CODEGEN GLUE: {sql_token} (funcName chain) */
            else if (funcName == "{sql_token}")
            {{
                if (helpers.top().constantBuilder.size() < {num_const_args})
                {{
                    throw InvalidQuerySyntax("{sql_token} requires {num_const_args} constant argument(s) at {{}}", context->getText());
                }}
                std::vector<std::string> constArgs({num_const_args});
                for (size_t i = 0; i < static_cast<size_t>({num_const_args}); ++i) {{
                    constArgs[static_cast<size_t>({num_const_args}) - 1 - i] = std::move(helpers.top().constantBuilder.back());
                    helpers.top().constantBuilder.pop_back();
                }}
                if (helpers.top().functionBuilder.size() < 4)
                {{
                    throw InvalidQuerySyntax("{sql_token} requires four field arguments at {{}}", context->getText());
                }}
                const auto ts = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto theta = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto y = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                const auto x = helpers.top().functionBuilder.back().get<FieldAccessLogicalFunction>();
                helpers.top().functionBuilder.pop_back();
                helpers.top().windowAggs.push_back({nebula_name}AggregationLogicalFunction::create(x, y, theta, ts, std::move(constArgs)));
            }}
            /* END CODEGEN GLUE: {sql_token} (funcName chain) */
"""

OPTIMIZER_LOWERING_TPOSE_CONST = """\
        /* BEGIN CODEGEN GLUE: {class_name_token} (optimizer lowering) */
        if (name == std::string_view("{class_name_token}"))
        {{
            auto specificDescriptor = std::dynamic_pointer_cast<{nebula_name}AggregationLogicalFunction>(descriptor);
            INVARIANT(specificDescriptor != nullptr, "Expected {nebula_name}AggregationLogicalFunction for {class_name_token}");

            auto xPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getXField());
            auto yPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getYField());
            auto thetaPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getThetaField());
            auto tsPF = QueryCompilation::FunctionProvider::lowerFunction(specificDescriptor->getTimestampField());

            Schema stateSchema;
            stateSchema.addField("x", specificDescriptor->getXField().getDataType());
            stateSchema.addField("y", specificDescriptor->getYField().getDataType());
            stateSchema.addField("theta", specificDescriptor->getThetaField().getDataType());
            stateSchema.addField("timestamp", specificDescriptor->getTimestampField().getDataType());
            auto tupleBufferRef = Interface::BufferRef::TupleBufferRef::create(configuration.pageSize.getValue(), stateSchema);

            auto phys = std::make_shared<{nebula_name}AggregationPhysicalFunction>(
                std::move(physicalInputType),
                std::move(physicalFinalType),
                xPF,
                yPF,
                thetaPF,
                tsPF,
                resultFieldIdentifier,
                tupleBufferRef,
                specificDescriptor->getConstArgs());
            aggregationPhysicalFunctions.push_back(std::move(phys));
            continue;
        }}
        /* END CODEGEN GLUE: {class_name_token} (optimizer lowering) */
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
            char* hexOut = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &hexSize);
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

# geometry value-output: f(traj) returns a GSERIALIZED (start/end point, convex
# hull, time-weighted centroid of the windowed trajectory), serialized as
# canonical hex-EWKB via geo_out. Same Temporal* slot/lift/append; only the
# finalize differs from the temporal value-output (GSERIALIZED + geo_out, no
# hexSize out-param).
_EXPAND_LOWER_GEO_WKB = _swap_once(
    _swap_once(_EXPAND_LOWER_WKB,
               "Temporal* res = {meos_scalar_fn}(*slot);",
               "GSERIALIZED* res = {meos_scalar_fn}(*slot);",
               "value-output res type Temporal -> GSERIALIZED"),
    "size_t hexSize = 0;\n            char* hexOut = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &hexSize);",
    "char* hexOut = geo_out(res);",
    "temporal hex-WKB -> geometry hex-EWKB (geo_out)")

PHYSICAL_CPP_TGEO_EXPAND_GEO_WKB = _swap_once(
    PHYSICAL_CPP_TGEO_EXPAND, _EXPAND_LOWER_SCALAR, _EXPAND_LOWER_GEO_WKB,
    "expand scalar lower -> geometry value-output (hex-EWKB) lower")

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
        if op.get("fold") == "tgeotransform":
            return PHYSICAL_HPP_TGEO_CONST, PHYSICAL_CPP_TGEO_CONST
        if op.get("return_mode") == "wkb":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_WKB
        if op.get("return_mode") == "expand":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_EXPAND
        if op.get("return_mode") == "expand_wkb":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_EXPAND_WKB
        if op.get("return_mode") == "expand_geo_wkb":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_EXPAND_GEO_WKB
        if op.get("return_mode") == "expand_wkb_tnpoint":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TNPOINT_EXPAND_WKB
        if op.get("fold") == "tagg":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_TAGG_WKB
        if op.get("fold") == "tgeoseq":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_SEQ
        if op.get("fold") == "tgeoseqtext":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_SEQ_TEXT
        if op.get("fold") == "tgeoseqarray":
            return PHYSICAL_HPP_TGEO, PHYSICAL_CPP_TGEO_SEQ_ARRAY
        return PHYSICAL_HPP_TGEO, (PHYSICAL_CPP_TGEO_BOX if box else PHYSICAL_CPP_TGEO)
    if op["input_shape"] == "tpose":
        if op.get("fold") == "tposetransform":
            return PHYSICAL_HPP_TPOSE_CONST, PHYSICAL_CPP_TPOSE_CONST
        if op.get("fold") == "tposeseqarray":
            return PHYSICAL_HPP_TPOSE, PHYSICAL_CPP_TPOSE_SEQ_ARRAY
        if op.get("fold") == "tposeseq1":
            return PHYSICAL_HPP_TPOSE, PHYSICAL_CPP_TPOSE_SEQ1
        return PHYSICAL_HPP_TPOSE, PHYSICAL_CPP_TPOSE
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
        if op.get("fold") == "arraymake":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_ARRAYMAKE
        if op.get("fold") == "objarraymake":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_OBJ_ARRAYMAKE
        if op.get("fold") == "tempmergearray":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_TEMPMERGEARRAY
        if op.get("fold") == "tseqsetmakegaps":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_TSEQSETMAKEGAPS
        if op.get("fold") == "spansetmake":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_SPANSETMAKE
        if op.get("fold") == "container":
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_CONTAINER_FOLD
        if op.get("fold") == "tagg":
            if op.get("tagg_value_kind") == "text":
                return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_TTEXT_TAGG_WKB
            return PHYSICAL_HPP_TNUMBER, PHYSICAL_CPP_TAGG_WKB
        return PHYSICAL_HPP_TNUMBER, (PHYSICAL_CPP_TNUMBER_BOX if box else PHYSICAL_CPP_TNUMBER)
    raise ValueError(f"unknown input_shape: {op['input_shape']}")


def logical_template_for(op):
    if op["input_shape"] == "tgeo":
        if op.get("fold") == "tgeotransform":
            return LOGICAL_HPP_TGEO_CONST, LOGICAL_CPP_TGEO_CONST
        return LOGICAL_HPP_TGEO, LOGICAL_CPP_TGEO
    if op["input_shape"] == "tpose":
        if op.get("fold") == "tposetransform":
            return LOGICAL_HPP_TPOSE_CONST, LOGICAL_CPP_TPOSE_CONST
        return LOGICAL_HPP_TPOSE, LOGICAL_CPP_TPOSE
    if op["input_shape"] == "tnumber":
        return LOGICAL_HPP_TNUMBER, LOGICAL_CPP_TNUMBER
    raise ValueError(f"unknown input_shape: {op['input_shape']}")


def case_switch_template_for(op):
    if op["input_shape"] == "tgeo":
        if op.get("fold") == "tgeotransform":
            return CASE_SWITCH_TGEO_CONST
        return CASE_SWITCH_TGEO
    if op["input_shape"] == "tpose":
        if op.get("fold") == "tposetransform":
            return CASE_SWITCH_TPOSE_CONST
        return CASE_SWITCH_TPOSE
    return CASE_SWITCH_TNUMBER


def funcname_chain_template_for(op):
    if op["input_shape"] == "tgeo":
        if op.get("fold") == "tgeotransform":
            return FUNCNAME_CHAIN_TGEO_CONST
        return FUNCNAME_CHAIN_TGEO
    if op["input_shape"] == "tpose":
        if op.get("fold") == "tposetransform":
            return FUNCNAME_CHAIN_TPOSE_CONST
        return FUNCNAME_CHAIN_TPOSE
    return FUNCNAME_CHAIN_TNUMBER


def optimizer_lowering_template_for(op):
    if op["input_shape"] == "tgeo":
        if op.get("fold") == "tgeotransform":
            return OPTIMIZER_LOWERING_TGEO_CONST
        return OPTIMIZER_LOWERING_TGEO
    if op["input_shape"] == "tpose":
        if op.get("fold") == "tposetransform":
            return OPTIMIZER_LOWERING_TPOSE_CONST
        return OPTIMIZER_LOWERING_TPOSE
    return OPTIMIZER_LOWERING_TNUMBER


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
        # array-make (fold=arraymake) extras — collect window values into a C
        # array then call the MEOS array constructor and serialize the result.
        "elem_cpp":            op.get("elem_cpp", "double"),
        "make_fn":             op.get("make_fn", ""),
        "make_ret_type":       op.get("make_ret_type", "Set"),
        "set_out_call":        op.get("set_out_call", ""),
        # temporal-aggregate (fold=tagg) extras — per-op transfn + finalfn.
        "tagg_transfn":        op.get("tagg_transfn", ""),
        "tagg_finalfn":        op.get("tagg_finalfn", ""),
        # value expression fed to sprintf (default the raw numeric field; tbool
        # maps it to the "t"/"f" literal MEOS expects).
        "value_expr":          op.get("value_expr", "valueVal"),
        # windowed-aggregate (w{min,max,sum}/wavg) extras: build a constant
        # Interval radius before the fold, pass it as the transfn's 3rd arg, free
        # it after. Empty for the plain t-aggregates.
        "tagg_pre_call":       op.get("tagg_pre_call", ""),
        "tagg_extra_arg":      op.get("tagg_extra_arg", ""),
        "tagg_post_call":      op.get("tagg_post_call", ""),
        # logical-op value-field type guard: numeric by default; VARSIZED-value
        # aggregates (ttext / container extent+union) relax it to timestamp-only.
        "value_type_guard":    op.get("value_type_guard", "!valueField.getDataType().isNumeric() || "),
        "value_type_msg":      op.get("value_type_msg", "value and timestamp fields must be numeric"),
        # container-fold (fold=container) bodies: parse `s` -> fold into `state`;
        # and finalize+serialize `state` -> char* (full per-op expressions).
        "container_fold_body":      op.get("container_fold_body", ""),
        "container_serialize_body": op.get("container_serialize_body", ""),
        # sequence-accessor (fold=tgeoseqtext) extra: the TSequence*-returning
        # accessor applied to the windowed continuous sequence `temp`.
        "seq_accessor_call":        op.get("seq_accessor_call", ""),
        # object-set array-make (fold=objarraymake) extras: the element parser
        # (*_in), any trailing parser arg (geom_in needs ", -1"), and the exact
        # pointer-array type the *set_make constructor expects (handles the const
        # on poseset_make).
        "elem_in":                  op.get("elem_in", ""),
        "elem_in_extra":            op.get("elem_in_extra", ""),
        "make_arg_type":            op.get("make_arg_type", op.get("elem_cpp", "void") + " **"),
        # tseqsetmakegaps (fold=tseqsetmakegaps) extras: the FIXED trailing make
        # args baked into the finalize — gap interpolation, the max-time interval
        # literal (parsed via interval_in), and the max spatial distance. These are
        # constants for the op, not user arguments.
        "gaps_interp":              op.get("gaps_interp", "LINEAR"),
        "gaps_maxt":                op.get("gaps_maxt", "1 hour"),
        "gaps_maxdist":             op.get("gaps_maxdist", "0.0"),
    }

    # Parameterized-transform (fold=tgeotransform) const-arg fragments: the per-const
    # parse block, the comma-prefixed C call args, any heap-free block, and the
    # finalize lambda's extra `const char*` params / nautilus invoke args. Empty for
    # every other op (harmless — the placeholders only appear in the const templates).
    fmt.update(render_const_args(op))

    # value_compute (point/tgeo finalize): either fold the windowed sequence
    # directly with meos_scalar_fn, or — for the EXTENT shape — first reduce the
    # sequence to its bounding box (tspatial_to_stbox / ...) and apply a box
    # accessor/predicate to that windowed extent. In box-output mode the
    # finalize is the serialized extent box itself (no value_compute).
    box_build = op.get("extent_box_build_fn")
    if op.get("return_mode") in ("box", "wkb"):
        fmt["value_compute"] = ""
    elif op.get("out_param_scalar"):
        # bool f(Temporal*, T* out): the useful result is the out-param scalar,
        # gated by the bool validity flag (e.g. tpoint_direction's azimuth). Seed
        # the value to 0 and let the call fill it; a false return leaves it 0.
        fmt["value_compute"] = (
            f'{op["return_cpp_type"]} value = ({op["return_cpp_type"]})0;\n'
            f'            {op["meos_scalar_fn"]}(static_cast<Temporal*>(temp), &value);')
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

    # Physical family aggregations live in a per-family subdir gated by the CMake
    # option; logical ops stay flat (never call libmeos).
    fam = meos_family(nebula_name)
    phys_sub = f"/{fam}" if fam else ""
    paths = {
        "logical_hpp":  output_root / "nes-logical-operators/include/Operators/Windows/Aggregations/Meos" / f"{nebula_name}AggregationLogicalFunction.hpp",
        "logical_cpp":  output_root / "nes-logical-operators/src/Operators/Windows/Aggregations/Meos" / f"{nebula_name}AggregationLogicalFunction.cpp",
        "physical_hpp": output_root / ("nes-physical-operators/include/Aggregation/Function/Meos" + phys_sub) / f"{nebula_name}AggregationPhysicalFunction.hpp",
        "physical_cpp": output_root / ("nes-physical-operators/src/Aggregation/Function/Meos" + phys_sub) / f"{nebula_name}AggregationPhysicalFunction.cpp",
    }
    for p in paths.values():
        p.parent.mkdir(parents=True, exist_ok=True)

    paths["logical_hpp"].write_text(logical_hpp_tmpl.format(**fmt))
    paths["logical_cpp"].write_text(logical_cpp_tmpl.format(**fmt))
    paths["physical_hpp"].write_text(physical_hpp_tmpl.format(**fmt))
    physical_cpp_text = physical_cpp_tmpl.format(**fmt)
    if fam:  # repoint the moved physical .cpp's self-include to its family subdir
        physical_cpp_text = physical_cpp_text.replace(
            f"<Aggregation/Function/Meos/{nebula_name}AggregationPhysicalFunction.hpp>",
            f"<Aggregation/Function/Meos/{fam}/{nebula_name}AggregationPhysicalFunction.hpp>")
    paths["physical_cpp"].write_text(physical_cpp_text)
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
    for label, parent, suffix in layers:
        if not parent.exists():
            sys.stderr.write(f"  ! cmake-entries: {parent} not found, skipping {label}\n")
            continue
        # Physical family aggregations route to a per-family subdir CMakeLists gated
        # by the family option; logical (and base physical) go to the flat parent.
        by_file = {}            # Path -> list[str]
        gated_families = set()
        for op in operators:
            # Target name must NOT include "Aggregation" suffix — the registry codegen
            # appends "Aggregation<RegistryKind>" itself, so a "...Aggregation" target
            # would yield a double-Aggregation symbol. The target name = the SQL-side
            # aggregation name (e.g. "TemporalLength"), NOT the C++ class basename.
            target_name = op["nebula_name"]
            registry_kind = "AggregationLogicalFunction" if label == "logical" else "AggregationPhysicalFunction"
            cpp_basename = f"{op['nebula_name']}{registry_kind}.cpp"
            entry = (
                f"add_plugin({target_name} {registry_kind} "
                f"nes-{label}-operators {cpp_basename})"
            )
            fam = meos_family(op["nebula_name"]) if label == "physical" else None
            if fam:
                cml = parent.parent / fam / "CMakeLists.txt"
                gated_families.add(fam)
            else:
                cml = parent
            by_file.setdefault(cml, []).append((entry, target_name, registry_kind))
        for cml, entries in by_file.items():
            cml.parent.mkdir(parents=True, exist_ok=True)
            body = cml.read_text() if cml.exists() else (
                f"# MEOS type-family aggregations, gated by the {cml.parent.name.upper()} compile option.\n\n")
            new_lines = [e for (e, tn, rk) in entries if f"add_plugin({tn} {rk}" not in body]
            if new_lines:
                with cml.open("a" if cml.exists() else "w") as f:
                    f.write(("" if cml.exists() else body) + "\n".join(new_lines) + "\n")
                n_added += len(new_lines)
        # Ensure the parent gates each new family subdir.
        pbody = parent.read_text()
        add = ""
        for fam in sorted(gated_families):
            if f"add_subdirectory({fam})" not in pbody:
                add += f"\nif({fam.upper()})\n    add_subdirectory({fam})\nendif()\n"
        if add:
            with parent.open("a") as f:
                f.write(add)
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


def _skip_past_family_endif(body: str, insert_at: int) -> int:
    """If the chosen insertion point sits immediately before a family-guard close
    `#endif /* CBUFFER|NPOINT|POSE|RGEO */`, advance past it. Mirrors the
    function-op generator (codegen_nebula.inject_parser_cpp): the last END marker
    may be the LAST block inside a family `#if <FAMILY> … #endif` guard, so
    inserting right after it would nest a (possibly generic) block inside the
    guard and leave the original `#endif` orphaned — unbalancing #if/#endif. The
    later guard pass re-wraps each family block in its own tight guard, so landing
    at switch scope here is always correct."""
    m_endif = re.match(r"\s*\n#endif /\* (?:CBUFFER|NPOINT|POSE|RGEO) \*/", body[insert_at:])
    return insert_at + m_endif.end() if m_endif else insert_at


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

    # 2) Case-switch dispatch — insert after the last `END CODEGEN GLUE: ... (case-switch)`
    #    marker, else before the `default:` of the switch that contains TGEO_AT_STBOX.
    new_case_blocks = []
    for op in operators:
        tmpl = case_switch_template_for(op)
        marker = f"/* BEGIN CODEGEN GLUE: {op['sql_token']} (case-switch) */"
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
            num_const_args=len(op.get("const_args", []) or []),
        ))
    if new_case_blocks:
        # Anchor preference order:
        #   1. last `END CODEGEN GLUE: ... (case-switch)` (own marker)
        #   2. last `END CODEGEN GLUE: ...` (codegen_nebula.py W4.5+)
        #   3. TGEO_AT_STBOX → default: (pre-W4.5 layout)
        last_end_agg = list(re.finditer(r"/\* END CODEGEN GLUE: [^*]+\(case-switch\)\s*\*/", body))
        last_end_nebula = list(re.finditer(r"/\* END CODEGEN GLUE: [^*(]+\*/", body))
        if last_end_agg:
            insert_at = _skip_past_family_endif(body, last_end_agg[-1].end())
            body = body[:insert_at] + "\n" + "\n".join(new_case_blocks) + body[insert_at:]
            sys.stderr.write(f"  ✓ parser-cpp case-switch: added {len(new_case_blocks)} (after own marker)\n")
        elif last_end_nebula:
            insert_at = _skip_past_family_endif(body, last_end_nebula[-1].end())
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

    # 3) funcName-chain dispatch — insert after the last `END CODEGEN GLUE: ... (funcName chain)`,
    #    else after mariana's CrossDistance else-if block.
    new_chain_blocks = []
    for op in operators:
        tmpl = funcname_chain_template_for(op)
        marker = f"/* BEGIN CODEGEN GLUE: {op['sql_token']} (funcName chain) */"
        if marker in body:
            continue
        if re.search(rf'funcName == "{re.escape(op["sql_token"])}"', body):
            sys.stderr.write(
                f"  ! parser-cpp: pre-existing funcName chain for {op['sql_token']}; skipping\n"
            )
            continue
        new_chain_blocks.append(tmpl.format(sql_token=op["sql_token"], nebula_name=op["nebula_name"],
                                            num_const_args=len(op.get("const_args", []) or [])))
    if new_chain_blocks:
        last_end_re = re.compile(r"/\* END CODEGEN GLUE: [^*]+\(funcName chain\)\s*\*/")
        ends = list(last_end_re.finditer(body))
        if ends:
            insert_at = _skip_past_family_endif(body, ends[-1].end())
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

    # Family-gate the dispatch glue + logical #includes via the SAME shared guard
    # the function-op generator uses (one mechanism, one `CODEGEN GLUE` marker), so
    # a -D<FAMILY>=0 build drops aggregate tokens too. Idempotent.
    import codegen_nebula
    body = codegen_nebula.guard_parser_glue_by_family(body)

    cpp_path.write_text(body)
    return n_added


def inject_optimizer(operators, opt_path: Path) -> int:
    """Inject `if (name == "...")` blocks into LowerToPhysicalWindowedAggregation.cpp."""
    if not opt_path.exists():
        sys.stderr.write(f"  ! optimizer: {opt_path} not found, skipping\n")
        return 0
    body = opt_path.read_text()
    n_added = 0

    # 1) #include for the physical class header. A family aggregation's physical
    # header lives in a per-family subdir and its class is compiled only when the
    # family is enabled, so guard the include with #if <FAMILY>.
    new_includes = []
    for op in operators:
        fam = meos_family(op["nebula_name"])
        sub = f"{fam}/" if fam else ""
        inc = f"#include <Aggregation/Function/Meos/{sub}{op['nebula_name']}AggregationPhysicalFunction.hpp>"
        if inc in body:
            continue
        if fam:
            inc = f"#if {fam.upper()}\n{inc}\n#endif"
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
        marker = f"/* BEGIN CODEGEN GLUE: {op['class_name_token']} (optimizer lowering) */"
        if marker in body:
            continue
        # Skip if a pre-existing hand-written block exists for this class_name_token
        if re.search(rf'name == std::string_view\("{re.escape(op["class_name_token"])}"\)', body):
            sys.stderr.write(
                f"  ! optimizer: pre-existing lowering block for {op['class_name_token']}; skipping\n"
            )
            continue
        block = tmpl.format(class_name_token=op["class_name_token"], nebula_name=op["nebula_name"])
        # The lowering block constructs the physical class directly, so a family
        # aggregation's block must be guarded with #if <FAMILY> (the physical class
        # is absent when the family is disabled).
        fam = meos_family(op["nebula_name"])
        if fam:
            block = f"#if {fam.upper()}\n{block}\n#endif"
        new_blocks.append(block)
    if new_blocks:
        last_end_re = re.compile(r"/\* END CODEGEN GLUE: [^*]+\(optimizer lowering\)\s*\*/")
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
