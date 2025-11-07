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

#include <string_view>

#include <Functions/FieldAccessLogicalFunction.hpp>
#include <SerializableVariantDescriptor.pb.h>

namespace NES::TemporalAggregationSerde
{
/// Key used to stash extra fields (lat, ts) for TemporalSequence inside the on_field SerializableFunction's config.
inline constexpr std::string_view TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY = "TemporalSequence.extra_fields";

/// Build a SerializableAggregationFunction for TemporalSequence storing lat/ts as a FunctionList inside on_field.config.
SerializableAggregationFunction serializeTemporalSequence(
    const FieldAccessLogicalFunction& lon,
    const FieldAccessLogicalFunction& lat,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField);

/// Parse lon, lat, ts, as FieldAccessLogicalFunctions from a SerializableAggregationFunction created by serializeTemporalSequence().
/// Returns fields in the order: lon, lat, ts, as.
std::vector<FieldAccessLogicalFunction> parseTemporalSequence(const SerializableAggregationFunction& saf);
}

