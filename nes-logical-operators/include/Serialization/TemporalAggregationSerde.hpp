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

#include <string>
#include <string_view>
#include <vector>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::TemporalAggregationSerde
{
/// Key used to stash extra fields (lat, ts) for TemporalSequence inside the on_field SerializableFunction's config.
inline constexpr std::string_view TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY = "TemporalSequence.extra_fields";

/// Key used to stash constant (non-field) scalar/object arguments for a parameterized
/// TemporalSequence-shaped aggregation inside the on_field SerializableFunction's config.
/// The constants are packed as a FunctionList of ConstantValueLogicalFunction (each holds
/// a DataType + a literal string), mirroring how extra fields are packed under
/// TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY. They are ALSO mirrored into the proto's native
/// `literals` repeated field so the existing deserializer (FunctionSerializationUtil) feeds
/// them straight into AggregationLogicalFunctionRegistryArguments.literals.
inline constexpr std::string_view TEMPORAL_SEQUENCE_CONST_ARGS_KEY = "TemporalSequence.const_args";

/// Pack the constant literal strings into `saf` under TEMPORAL_SEQUENCE_CONST_ARGS_KEY as a
/// FunctionList of ConstantValueLogicalFunction, and mirror them into `saf.literals`. Call
/// AFTER serializeTemporalSequence() (which sets on_field). A no-op when `constArgs` is empty.
void serializeConstArgs(SerializableAggregationFunction& saf, const std::vector<std::string>& constArgs);

/// Extract the constant literal strings packed by serializeConstArgs() (reads the
/// TEMPORAL_SEQUENCE_CONST_ARGS_KEY FunctionList; returns empty if absent).
std::vector<std::string> parseConstArgs(const SerializableAggregationFunction& saf);

/// Build a SerializableAggregationFunction for TemporalSequence storing lat/ts as a FunctionList inside on_field.config.
SerializableAggregationFunction serializeTemporalSequence(
    const FieldAccessLogicalFunction& lon,
    const FieldAccessLogicalFunction& lat,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField);

/// Four-data-field (x, y, theta, ts) overload for the tpose shape: packs
/// (y, theta, ts) as 3 extras inside on_field.config so parseTemporalSequence()
/// (which loops over any number of extras) round-trips [x, y, theta, ts, as].
SerializableAggregationFunction serializeTemporalSequence(
    const FieldAccessLogicalFunction& x,
    const FieldAccessLogicalFunction& y,
    const FieldAccessLogicalFunction& theta,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField);

/// Parse lon, lat, ts, as FieldAccessLogicalFunctions from a SerializableAggregationFunction created by serializeTemporalSequence().
/// Returns fields in the order: lon, lat, ts, as.
std::vector<FieldAccessLogicalFunction> parseTemporalSequence(const SerializableAggregationFunction& saf);
}

