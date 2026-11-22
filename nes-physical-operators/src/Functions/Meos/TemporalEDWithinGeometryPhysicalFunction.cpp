#include <Functions/Meos/TemporalEDWithinGeometryPhysicalFunction.hpp>

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
#include <iostream>
#include <string>
#include <utility>
#include <val.hpp>

namespace NES {

TemporalEDWithinGeometryPhysicalFunction::TemporalEDWithinGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                                                                   PhysicalFunction latFunction,
                                                                                   PhysicalFunction timestampFunction,
                                                                                   PhysicalFunction geometryFunction,
                                                                                   PhysicalFunction distanceFunction)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(geometryFunction));
    parameterFunctions.push_back(std::move(distanceFunction));
}

VarVal TemporalEDWithinGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
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
    auto geometry = parameterValues[3].cast<VariableSizedData>();
    auto distance = parameterValues[4].cast<nautilus::val<double>>();

    const auto result = nautilus::invoke(
        +[](double lonValue,
            double latValue,
            uint64_t timestampValue,
            const char* geometryPtr,
            uint32_t geometrySize,
            double distanceValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lonValue >= -180.0 && lonValue <= 180.0 && latValue >= -90.0 && latValue <= 90.0)) {
                    std::cout << "TemporalEDWithin: coordinates out of range" << std::endl;
                    return 0;
                }

                const std::string timestampString = MEOS::Meos::convertEpochToTimestamp(timestampValue);
                std::string temporalGeometryWkt = fmt::format("SRID=4326;Point({} {})@{}", lonValue, latValue, timestampString);
                std::string staticGeometryWkt(geometryPtr, geometrySize);

                while (!staticGeometryWkt.empty() && (staticGeometryWkt.front() == '\'' || staticGeometryWkt.front() == '"'))
                    staticGeometryWkt.erase(staticGeometryWkt.begin());
                while (!staticGeometryWkt.empty() && (staticGeometryWkt.back() == '\'' || staticGeometryWkt.back() == '"'))
                    staticGeometryWkt.pop_back();

                if (temporalGeometryWkt.empty() || staticGeometryWkt.empty())
                    return 0;

                MEOS::Meos::TemporalGeometry temporalGeometry(temporalGeometryWkt);
                if (!temporalGeometry.getGeometry()) return 0;
                MEOS::Meos::StaticGeometry staticGeometry(staticGeometryWkt);
                if (!staticGeometry.getGeometry()) return 0;

                return MEOS::Meos::safe_edwithin_tgeo_geo(static_cast<const Temporal*>(temporalGeometry.getGeometry()),
                                                         static_cast<const GSERIALIZED*>(staticGeometry.getGeometry()),
                                                         distanceValue);
            }
            catch (...) { return -1; }
        },
        lon, lat, timestamp, geometry.getContent(), geometry.getContentSize(), distance);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType
PhysicalFunctionGeneratedRegistrar::RegisterTemporalEDWithinGeometryPhysicalFunction(PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "TemporalEDWithinGeometryPhysicalFunction requires 5 child functions, but got {}",
                 arguments.childFunctions.size());
    return TemporalEDWithinGeometryPhysicalFunction(arguments.childFunctions[0],
                                                    arguments.childFunctions[1],
                                                    arguments.childFunctions[2],
                                                    arguments.childFunctions[3],
                                                    arguments.childFunctions[4]);
}

} // namespace NES
