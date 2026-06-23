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

#include <Functions/Meos/EverEqTcbufferCbufferPhysicalFunction.hpp>
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
#include <stdlib.h>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
#include <meos_cbuffer.h>
}

namespace NES {

EverEqTcbufferCbufferPhysicalFunction::EverEqTcbufferCbufferPhysicalFunction(
    PhysicalFunction lon1, PhysicalFunction lat1, PhysicalFunction r1,
    PhysicalFunction ts,
    PhysicalFunction lon2, PhysicalFunction lat2, PhysicalFunction r2)
{
    paramFns.reserve(7);
    paramFns.push_back(std::move(lon1));
    paramFns.push_back(std::move(lat1));
    paramFns.push_back(std::move(r1));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(lon2));
    paramFns.push_back(std::move(lat2));
    paramFns.push_back(std::move(r2));
}

VarVal EverEqTcbufferCbufferPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto lon1 = paramFns[0].execute(record, arena).cast<double>();
    auto lat1 = paramFns[1].execute(record, arena).cast<double>();
    auto r1   = paramFns[2].execute(record, arena).cast<double>();
    auto ts   = paramFns[3].execute(record, arena).cast<uint64_t>();
    auto lon2 = paramFns[4].execute(record, arena).cast<double>();
    auto lat2 = paramFns[5].execute(record, arena).cast<double>();
    auto r2   = paramFns[6].execute(record, arena).cast<double>();

    const auto result = nautilus::invoke(
        +[](double lon1, double lat1, double r1, uint64_t ts,
            double lon2, double lat2, double r2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                // Build tcbuffer instant from (lon1,lat1,r1,ts)
                std::string pt1 = fmt::format("POINT({} {})", lon1, lat1);
                GSERIALIZED* pt1_gs = geom_in(pt1.c_str(), -1);
                if (!pt1_gs) return 0.0;
                Cbuffer* cb1 = cbuffer_make(pt1_gs, r1);
                free(pt1_gs);
                if (!cb1) return 0.0;
                Temporal* tcb_inst = (Temporal*)tcbufferinst_make(cb1, (TimestampTz)ts);
                free(cb1);
                if (!tcb_inst) return 0.0;
                // Build static cbuffer from (lon2,lat2,r2)
                std::string pt2 = fmt::format("POINT({} {})", lon2, lat2);
                GSERIALIZED* pt2_gs = geom_in(pt2.c_str(), -1);
                if (!pt2_gs) { free(tcb_inst); return 0.0; }
                Cbuffer* cb2 = cbuffer_make(pt2_gs, r2);
                free(pt2_gs);
                if (!cb2) { free(tcb_inst); return 0.0; }
                int r = ever_eq_tcbuffer_cbuffer(tcb_inst, cb2);
                free(tcb_inst);
                free(cb2);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        lon1, lat1, r1, ts, lon2, lat2, r2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverEqTcbufferCbufferPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 7,
                 "EverEqTcbufferCbufferPhysicalFunction requires 7 children but got {}",
                 arguments.childFunctions.size());
    return EverEqTcbufferCbufferPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]),
                                  std::move(arguments.childFunctions[6]));
}

} // namespace NES
