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

#include <Functions/Meos/NadTnpointGeoPhysicalFunction.hpp>
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
#include <meos_npoint.h>
}

namespace NES {

NadTnpointGeoPhysicalFunction::NadTnpointGeoPhysicalFunction(PhysicalFunction rid, PhysicalFunction pos, PhysicalFunction ts, PhysicalFunction wkt)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(rid));
    paramFns.push_back(std::move(pos));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(wkt));
}

VarVal NadTnpointGeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto rid = paramFns[0].execute(record, arena).cast<uint64_t>();
    auto pos = paramFns[1].execute(record, arena).cast<double>();
    auto ts = paramFns[2].execute(record, arena).cast<uint64_t>();
    auto wkt = paramFns[3].execute(record, arena);
    const auto result = nautilus::invoke(
        +[](uint64_t rid, double pos, uint64_t ts, const char* wkt, uint32_t wkt_len) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Npoint* np = npoint_make((int64_t)rid, pos);
                if (!np) return 0.0;
                Temporal* inst = (Temporal*)tnpointinst_make(np, (TimestampTz)ts);
                free(np);
                if (!inst) return 0.0;
                char* wkt_str = (char*)malloc(wkt_len + 1);
                memcpy(wkt_str, wkt, wkt_len);
                wkt_str[wkt_len] = '\0';
                GSERIALIZED* gs = geom_in(wkt_str, -1);
                free(wkt_str);
                if (!gs) { free(inst); return 0.0; }
                double r = nad_tnpoint_geo(inst, gs);
                free(inst); free(gs);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        rid, pos, ts, wkt);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNadTnpointGeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "NadTnpointGeoPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return NadTnpointGeoPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
