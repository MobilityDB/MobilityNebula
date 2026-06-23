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

#include <Functions/Meos/AcoversGeoTgeoPhysicalFunction.hpp>
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

AcoversGeoTgeoPhysicalFunction::AcoversGeoTgeoPhysicalFunction(PhysicalFunction wkt, PhysicalFunction lon,
                                              PhysicalFunction lat, PhysicalFunction ts)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(wkt));
    paramFns.push_back(std::move(lon));
    paramFns.push_back(std::move(lat));
    paramFns.push_back(std::move(ts));
}

VarVal AcoversGeoTgeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto lon = paramFns[1].execute(record, arena).cast<double>();
    auto lat = paramFns[2].execute(record, arena).cast<double>();
    auto ts  = paramFns[3].execute(record, arena).cast<uint64_t>();

    const auto result = nautilus::invoke(
        +[](const char* w, uint32_t wsz,
            double lon, double lat, uint64_t ts) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string wktStr(w, wsz);
                GSERIALIZED* gs = geom_in(wktStr.c_str(), -1);
                if (!gs) return 0.0;
                std::string tgeoWkt = fmt::format("SRID=4326;POINT({},{})@{}", lon, lat, ts);
                Temporal* tgeo = tgeompoint_in(tgeoWkt.c_str());
                if (!tgeo) { free(gs); return 0.0; }
                int r = acovers_geo_tgeo(gs, tgeo);
                free(gs); free(tgeo);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        wkt, lon, lat, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAcoversGeoTgeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "AcoversGeoTgeoPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return AcoversGeoTgeoPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
