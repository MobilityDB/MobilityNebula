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

#include <Functions/Meos/TemporalEDisjointGeometryPhysicalFunction.hpp>

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
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace NES {

TemporalEDisjointGeometryPhysicalFunction::TemporalEDisjointGeometryPhysicalFunction(PhysicalFunction lonFunction,
                                                          PhysicalFunction latFunction,
                                                          PhysicalFunction timestampFunction,
                                                          PhysicalFunction geometryFunction)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(geometryFunction));
}

VarVal TemporalEDisjointGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon       = parameterValues[0].cast<nautilus::val<double>>();
    auto lat       = parameterValues[1].cast<nautilus::val<double>>();
    auto timestamp = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto geometry  = parameterValues[3].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double lonValue,
            double latValue,
            uint64_t timestampValue,
            const char* geometryPtr,
            uint32_t geometrySize) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lonValue >= -180.0 && lonValue <= 180.0 && latValue >= -90.0 && latValue <= 90.0)) {
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

                // MEOS spatial-relation call — same shape as TemporalEDWithin's
                // edwithin_tgeo_geo, but specific MEOS function per generated operator.
                // Real MEOS spatial-rel signature: int fn(const Temporal *, const GSERIALIZED *)
                // (no `atstart` flag — that's specific to geog_dwithin / edwithin's 3-arg variant).
                return edisjoint_tgeo_geo(temporalGeometry.getGeometry(),
                                   staticGeometry.getGeometry());
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        lon, lat, timestamp, geometry.getContent(), geometry.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTemporalEDisjointGeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "TemporalEDisjointGeometryPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    return TemporalEDisjointGeometryPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
