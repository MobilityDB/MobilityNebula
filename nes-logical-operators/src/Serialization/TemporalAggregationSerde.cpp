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

#include <Serialization/TemporalAggregationSerde.hpp>

#include <Configurations/Descriptor.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Serialization/FunctionSerializationUtil.hpp>

namespace NES::TemporalAggregationSerde
{

SerializableAggregationFunction serializeTemporalSequence(
    const FieldAccessLogicalFunction& lon,
    const FieldAccessLogicalFunction& lat,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField)
{
    SerializableAggregationFunction saf;
    saf.set_type("TemporalSequence");

    // on_field: longitude
    SerializableFunction lonProto;
    lonProto.CopyFrom(LogicalFunction(lon).serialize());

    // Pack extra fields (lat, ts) into on_field.config as a FunctionList
    FunctionList extraList;
    *extraList.add_functions() = LogicalFunction(lat).serialize();
    *extraList.add_functions() = LogicalFunction(ts).serialize();

    // Convert FunctionList to SerializableVariantDescriptor and attach under our key
    const auto key = std::string(TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY);
    (*lonProto.mutable_config())[key] = descriptorConfigTypeToProto(extraList);
    saf.mutable_on_field()->CopyFrom(lonProto);

    // as_field: alias
    SerializableFunction asProto;
    asProto.CopyFrom(LogicalFunction(asField).serialize());
    saf.mutable_as_field()->CopyFrom(asProto);

    return saf;
}

std::vector<FieldAccessLogicalFunction> parseTemporalSequence(const SerializableAggregationFunction& saf)
{
    std::vector<FieldAccessLogicalFunction> out;
    // lon
    const auto lonFn = FunctionSerializationUtil::deserializeFunction(saf.on_field());
    if (auto lon = lonFn.tryGet<FieldAccessLogicalFunction>())
    {
        out.push_back(*lon);
    }
    else
    {
        throw CannotDeserialize("TemporalSequence: on_field is not FieldAccessLogicalFunction");
    }

    // extra fields from on_field.config
    const auto key = std::string(TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY);
    const auto& onFieldCfg = saf.on_field().config();
    if (onFieldCfg.contains(key))
    {
        const auto variant = protoToDescriptorConfigType(onFieldCfg.at(key));
        if (std::holds_alternative<FunctionList>(variant))
        {
            const auto list = std::get<FunctionList>(variant);
            for (const auto& f : list.functions())
            {
                const auto lf = FunctionSerializationUtil::deserializeFunction(f);
                if (auto access = lf.tryGet<FieldAccessLogicalFunction>())
                {
                    out.push_back(*access);
                }
                else
                {
                    throw CannotDeserialize("TemporalSequence: extra field is not FieldAccessLogicalFunction");
                }
            }
        }
    }

    // as field (alias) comes as regular as_field
    const auto asFn = FunctionSerializationUtil::deserializeFunction(saf.as_field());
    if (auto as = asFn.tryGet<FieldAccessLogicalFunction>())
    {
        out.push_back(*as);
    }
    else
    {
        throw CannotDeserialize("TemporalSequence: as_field is not FieldAccessLogicalFunction");
    }

    return out;
}

} // namespace NES::TemporalAggregationSerde

