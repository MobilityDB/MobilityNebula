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

#include <Functions/Meos/EcontainsTcbufferGeoPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <utility>
#include <val.hpp>
#include <fmt/format.h>
#include <string.h>
#include <stdlib.h>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_cbuffer.h>
}

namespace NES {

EcontainsTcbufferGeoPhysicalFunction::EcontainsTcbufferGeoPhysicalFunction(PhysicalFunction lon, PhysicalFunction lat,
                                              PhysicalFunction radius, PhysicalFunction ts,
                                              PhysicalFunction wkt)
{
    paramFns.reserve(5);
    paramFns.push_back(std::move(lon));
    paramFns.push_back(std::move(lat));
    paramFns.push_back(std::move(radius));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(wkt));
}

VarVal EcontainsTcbufferGeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto lon    = paramFns[0].execute(record, arena).cast<double>();
    auto lat    = paramFns[1].execute(record, arena).cast<double>();
    auto radius = paramFns[2].execute(record, arena).cast<double>();
    auto ts     = paramFns[3].execute(record, arena).cast<uint64_t>();
    auto wkt    = paramFns[4].execute(record, arena);

    const auto result = nautilus::invoke(
        +[](double lon, double lat, double radius, uint64_t ts,
            const char* wkt, uint32_t wkt_len) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                // Build tcbuffer instant: point + radius → cbuffer → instant
                std::string pt_str = fmt::format("POINT({} {})", lon, lat);
                GSERIALIZED* pt_gs = geom_in(pt_str.c_str(), -1);
                if (!pt_gs) return 0.0;
                Cbuffer* cb = cbuffer_make(pt_gs, radius);
                free(pt_gs);
                if (!cb) return 0.0;
                Temporal* tcb_inst = (Temporal*)tcbufferinst_make(cb, (TimestampTz)ts);
                free(cb);
                if (!tcb_inst) return 0.0;
                // Build target geometry from null-terminated WKT copy
                char* wkt_str = (char*)malloc(wkt_len + 1);
                memcpy(wkt_str, wkt, wkt_len);
                wkt_str[wkt_len] = '\0';
                GSERIALIZED* target_gs = geom_in(wkt_str, -1);
                free(wkt_str);
                if (!target_gs) { free(tcb_inst); return 0.0; }
                int r = econtains_tcbuffer_geo(tcb_inst, target_gs);
                free(tcb_inst);
                free(target_gs);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        lon, lat, radius, ts, wkt);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEcontainsTcbufferGeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "EcontainsTcbufferGeoPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    return EcontainsTcbufferGeoPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]));
}

} // namespace NES
