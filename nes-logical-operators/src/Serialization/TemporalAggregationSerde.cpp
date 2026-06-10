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

#include <string>
#include <vector>

#include <Configurations/Descriptor.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
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

SerializableAggregationFunction serializeTemporalSequence(
    const FieldAccessLogicalFunction& x,
    const FieldAccessLogicalFunction& y,
    const FieldAccessLogicalFunction& theta,
    const FieldAccessLogicalFunction& ts,
    const FieldAccessLogicalFunction& asField)
{
    SerializableAggregationFunction saf;
    saf.set_type("TemporalSequence");

    // on_field: x
    SerializableFunction xProto;
    xProto.CopyFrom(LogicalFunction(x).serialize());

    // Pack extra fields (y, theta, ts) into on_field.config as a FunctionList.
    // parseTemporalSequence() loops over any number of extras, so this round-
    // trips as [x, y, theta, ts, as].
    FunctionList extraList;
    *extraList.add_functions() = LogicalFunction(y).serialize();
    *extraList.add_functions() = LogicalFunction(theta).serialize();
    *extraList.add_functions() = LogicalFunction(ts).serialize();

    const auto key = std::string(TEMPORAL_SEQUENCE_EXTRA_FIELDS_KEY);
    (*xProto.mutable_config())[key] = descriptorConfigTypeToProto(extraList);
    saf.mutable_on_field()->CopyFrom(xProto);

    // as_field: alias
    SerializableFunction asProto;
    asProto.CopyFrom(LogicalFunction(asField).serialize());
    saf.mutable_as_field()->CopyFrom(asProto);

    return saf;
}

void serializeConstArgs(SerializableAggregationFunction& saf, const std::vector<std::string>& constArgs)
{
    if (constArgs.empty())
    {
        return;
    }

    // Pack each constant literal as a ConstantValueLogicalFunction into a FunctionList and
    // attach it under our key inside on_field.config — mirroring how serializeTemporalSequence
    // packs the extra fields. The DataType is a placeholder (the physical layer parses the
    // literal string to its real C type); the literal string is the payload.
    FunctionList constList;
    for (const auto& c : constArgs)
    {
        const ConstantValueLogicalFunction constFn(DataTypeProvider::provideDataType(DataType::Type::VARSIZED), c);
        *constList.add_functions() = LogicalFunction(constFn).serialize();
    }

    const auto key = std::string(TEMPORAL_SEQUENCE_CONST_ARGS_KEY);
    (*saf.mutable_on_field()->mutable_config())[key] = descriptorConfigTypeToProto(constList);

    // Mirror into the proto's native `literals` repeated field so the existing
    // deserializeWindowAggregationFunction() feeds them straight into args.literals.
    for (const auto& c : constArgs)
    {
        saf.add_literals(c);
    }
}

std::vector<std::string> parseConstArgs(const SerializableAggregationFunction& saf)
{
    std::vector<std::string> out;
    const auto key = std::string(TEMPORAL_SEQUENCE_CONST_ARGS_KEY);
    const auto& onFieldCfg = saf.on_field().config();
    if (!onFieldCfg.contains(key))
    {
        return out;
    }
    const auto variant = protoToDescriptorConfigType(onFieldCfg.at(key));
    if (std::holds_alternative<FunctionList>(variant))
    {
        const auto list = std::get<FunctionList>(variant);
        for (const auto& f : list.functions())
        {
            const auto lf = FunctionSerializationUtil::deserializeFunction(f);
            if (auto cst = lf.tryGet<ConstantValueLogicalFunction>())
            {
                out.push_back(cst->getConstantValue());
            }
            else
            {
                throw CannotDeserialize("TemporalSequence: const arg is not ConstantValueLogicalFunction");
            }
        }
    }
    return out;
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

