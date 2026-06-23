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

#include <Functions/Meos/NadTgeoGeoPhysicalFunction.hpp>
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

NadTgeoGeoPhysicalFunction::NadTgeoGeoPhysicalFunction(PhysicalFunction lon, PhysicalFunction lat,
                                              PhysicalFunction ts, PhysicalFunction wkt)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(lon));
    paramFns.push_back(std::move(lat));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(wkt));
}

VarVal NadTgeoGeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto lon = paramFns[0].execute(record, arena).cast<double>();
    auto lat = paramFns[1].execute(record, arena).cast<double>();
    auto ts  = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto wkt = paramFns[3].execute(record, arena).cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double lon, double lat, uint64_t ts,
            const char* w, uint32_t wsz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string wktStr(w, wsz);
                std::string tgeoWkt = fmt::format("SRID=4326;POINT({},{})@{}", lon, lat, ts);
                Temporal* tgeo = tgeompoint_in(tgeoWkt.c_str());
                if (!tgeo) return 0.0;
                GSERIALIZED* gs = geom_in(wktStr.c_str(), -1);
                if (!gs) { free(tgeo); return 0.0; }
                double r = nad_tgeo_geo(tgeo, gs);
                free(tgeo); free(gs);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        lon, lat, ts, wkt);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNadTgeoGeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "NadTgeoGeoPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return NadTgeoGeoPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
