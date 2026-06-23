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

#include <Functions/Meos/EintersectsTpcpointGeoPhysicalFunction.hpp>
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
#include <stdlib.h>
#include <string.h>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_pointcloud.h>
}

namespace NES {

EintersectsTpcpointGeoPhysicalFunction::EintersectsTpcpointGeoPhysicalFunction(PhysicalFunction pt_hexwkb, PhysicalFunction ts, PhysicalFunction tgt_wkt)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(pt_hexwkb));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(tgt_wkt));
}

VarVal EintersectsTpcpointGeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto pt_hexwkb = paramFns[0].execute(record, arena);
    auto ts = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto tgt_wkt = paramFns[2].execute(record, arena);
    const auto result = nautilus::invoke(
        +[](const char* pt_hexwkb, uint32_t pt_len, uint64_t ts, const char* tgt_wkt, uint32_t tgt_len) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* hs = (char*)malloc(pt_len + 1);
                memcpy(hs, pt_hexwkb, pt_len); hs[pt_len] = '\0';
                Pcpoint* pt = pcpoint_from_hexwkb(hs); free(hs);
                if (!pt) return 0.0;
                TInstant* inst = tpointcloudinst_make(pt, (TimestampTz)ts);
                free(pt);
                if (!inst) return 0.0;
                char* gs_str = (char*)malloc(tgt_len + 1);
                memcpy(gs_str, tgt_wkt, tgt_len); gs_str[tgt_len] = '\0';
                GSERIALIZED* gs = geom_in(gs_str, -1); free(gs_str);
                if (!gs) { free(inst); return 0.0; }
                bool r = eintersects_tpcpoint_geo((Temporal*)inst, gs);
                free(inst); free(gs);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        pt_hexwkb, ts, tgt_wkt);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEintersectsTpcpointGeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "EintersectsTpcpointGeoPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return EintersectsTpcpointGeoPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
