#include <Functions/Meos/TemporalAtStBoxPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <cctype>
#include <iostream>
#include <string>
#include <utility>
#include <val.hpp>

namespace NES {

TemporalAtStBoxPhysicalFunction::TemporalAtStBoxPhysicalFunction(PhysicalFunction lonFunction,
                                                                 PhysicalFunction latFunction,
                                                                 PhysicalFunction timestampFunction,
                                                                 PhysicalFunction stboxFunction)
    : hasBorderParam(false)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(stboxFunction));
}

TemporalAtStBoxPhysicalFunction::TemporalAtStBoxPhysicalFunction(PhysicalFunction lonFunction,
                                                                 PhysicalFunction latFunction,
                                                                 PhysicalFunction timestampFunction,
                                                                 PhysicalFunction stboxFunction,
                                                                 PhysicalFunction borderInclusiveFunction)
    : hasBorderParam(true)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(stboxFunction));
    parameterFunctions.push_back(std::move(borderInclusiveFunction));
}

VarVal TemporalAtStBoxPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon = parameterValues[0].cast<nautilus::val<double>>();
    auto lat = parameterValues[1].cast<nautilus::val<double>>();
    auto timestamp = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto stboxLiteral = parameterValues[3].cast<VariableSizedData>();

    nautilus::val<bool> borderVal = nautilus::val<bool>(true);
    if (hasBorderParam && parameterValues.size() >= 5)
    {
        borderVal = parameterValues[4].cast<nautilus::val<bool>>();
    }

    const auto result = nautilus::invoke(
        +[](double lonValue,
            double latValue,
            uint64_t timestampValue,
            const char* stboxPtr,
            uint32_t stboxSize,
            bool borderInclusiveFlag) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                const std::string timestampString = MEOS::Meos::convertSecondsToTimestamp(timestampValue);
                std::string temporalGeometryWkt = fmt::format("SRID=4326;Point({} {})@{}", lonValue, latValue, timestampString);
                std::string stboxWkt(stboxPtr, stboxSize);
                while (!stboxWkt.empty() && (stboxWkt.front()=='\'' || stboxWkt.front()=='"')) stboxWkt.erase(stboxWkt.begin());
                while (!stboxWkt.empty() && (stboxWkt.back()=='\'' || stboxWkt.back()=='"')) stboxWkt.pop_back();
                if (temporalGeometryWkt.empty() || stboxWkt.empty()) return 0;

                MEOS::Meos::TemporalGeometry temporalGeometry(temporalGeometryWkt);
                if (!temporalGeometry.getGeometry()) return 0;
                MEOS::Meos::SpatioTemporalBox stbox(stboxWkt);
                if (!stbox.getBox()) return 0;
                MEOS::Meos::TemporalHolder clipped(MEOS::Meos::safe_tgeo_at_stbox(temporalGeometry.getGeometry(), stbox.getBox(), borderInclusiveFlag));
                return clipped.get() != nullptr ? 1 : 0;
            }
            catch (...) { return -1; }
        },
        lon,
        lat,
        timestamp,
        stboxLiteral.getContent(),
        stboxLiteral.getContentSize(),
        borderVal);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalAtStBoxPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    if (arguments.childFunctions.size() == 4)
    {
        return TemporalAtStBoxPhysicalFunction(arguments.childFunctions[0],
                                                arguments.childFunctions[1],
                                                arguments.childFunctions[2],
                                                arguments.childFunctions[3]);
    }
    if (arguments.childFunctions.size() == 5)
    {
        return TemporalAtStBoxPhysicalFunction(arguments.childFunctions[0],
                                                arguments.childFunctions[1],
                                                arguments.childFunctions[2],
                                                arguments.childFunctions[3],
                                                arguments.childFunctions[4]);
    }
    PRECONDITION(false,
                 "TemporalAtStBoxPhysicalFunction requires 4 or 5 child functions, but got {}",
                 arguments.childFunctions.size());
}

} // namespace NES
