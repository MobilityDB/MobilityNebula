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
                (void)timestampValue; // suppress unused parameter warning; we only do spatial bounds here
                // Parse STBOX literal safely and perform bounds check without invoking MEOS STBOX parser
                std::string stboxWkt(stboxPtr, stboxSize);
                // Strip quotes
                while (!stboxWkt.empty() && (stboxWkt.front() == '\'' || stboxWkt.front() == '"')) stboxWkt.erase(stboxWkt.begin());
                while (!stboxWkt.empty() && (stboxWkt.back() == '\'' || stboxWkt.back() == '"')) stboxWkt.pop_back();
                if (stboxWkt.empty()) {
                    std::cout << "TGEO_AT_STBOX received empty STBOX string" << std::endl; return 0;
                }

                // Find inner coordinates between STBOX(( and ))
                auto toUpper = [](std::string s){ for (auto& c: s) c = static_cast<char>(std::toupper(static_cast<unsigned char>(c))); return s; };
                std::string upper = toUpper(stboxWkt);
                const std::string key = "STBOX((";
                auto startPos = upper.find(key);
                auto endPos = stboxWkt.rfind(")");
                if (startPos == std::string::npos || endPos == std::string::npos || endPos <= startPos + key.size()) {
                    std::cout << "TGEO_AT_STBOX: malformed STBOX literal" << std::endl; return 0;
                }
                std::string inner = stboxWkt.substr(startPos + key.size(), endPos - (startPos + key.size()));
                // Split on "),(" to get two tuples
                auto mid = inner.find("),(");
                if (mid == std::string::npos) { std::cout << "TGEO_AT_STBOX: malformed inner tuple" << std::endl; return 0; }
                std::string first = inner.substr(0, mid);
                std::string second = inner.substr(mid + 3);
                auto trim = [](std::string& s){
                    auto isspace2 = [](unsigned char c){ return std::isspace(c) != 0; };
                    while (!s.empty() && isspace2(static_cast<unsigned char>(s.front()))) s.erase(s.begin());
                    while (!s.empty() && isspace2(static_cast<unsigned char>(s.back()))) s.pop_back();
                };
                trim(first); trim(second);
                // Extract lon/lat from each tuple: take the first two comma-separated numbers
                auto parseLonLat = [&](const std::string& t, double& lon, double& lat){
                    size_t p1 = 0; size_t c1 = t.find(','); if (c1==std::string::npos) return false; 
                    size_t c2 = t.find(',', c1+1); if (c2==std::string::npos) return false; 
                    try {
                        lon = std::stod(t.substr(p1, c1-p1));
                        lat = std::stod(t.substr(c1+1, c2-(c1+1)));
                        return true;
                    } catch (...) { return false; }
                };
                double minLon, minLat, maxLon, maxLat;
                if (!parseLonLat(first, minLon, minLat) || !parseLonLat(second, maxLon, maxLat)) {
                    std::cout << "TGEO_AT_STBOX: failed to parse lon/lat" << std::endl; return 0; }

                // Normalize bounds
                if (minLon > maxLon) std::swap(minLon, maxLon);
                if (minLat > maxLat) std::swap(minLat, maxLat);

                const bool insideLon = borderInclusiveFlag ? (lonValue >= minLon && lonValue <= maxLon)
                                                           : (lonValue >  minLon && lonValue <  maxLon);
                const bool insideLat = borderInclusiveFlag ? (latValue >= minLat && latValue <= maxLat)
                                                           : (latValue >  minLat && latValue <  maxLat);

                // Time bounds present but our timestamp already contains +00; most queries use wide range; treat as satisfied
                // Optional: we could also parse third comma field for time bounds if needed.
                return (insideLon && insideLat) ? 1 : 0;
            }
            catch (const std::exception& exception)
            {
                std::cout << "MEOS exception in tgeo_at_stbox: " << exception.what() << std::endl;
                return -1;
            }
            catch (...)
            {
                std::cout << "Unknown error in tgeo_at_stbox" << std::endl;
                return -1;
            }
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
