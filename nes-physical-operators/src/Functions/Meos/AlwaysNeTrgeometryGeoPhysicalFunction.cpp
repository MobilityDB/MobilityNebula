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

#include <Functions/Meos/AlwaysNeTrgeometryGeoPhysicalFunction.hpp>
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
#include <meos_pose.h>
#include <meos_rgeo.h>
}

namespace NES {

AlwaysNeTrgeometryGeoPhysicalFunction::AlwaysNeTrgeometryGeoPhysicalFunction(PhysicalFunction ref_wkt, PhysicalFunction x, PhysicalFunction y, PhysicalFunction theta, PhysicalFunction ts, PhysicalFunction tgt_wkt)
{
    paramFns.reserve(6);
    paramFns.push_back(std::move(ref_wkt));
    paramFns.push_back(std::move(x));
    paramFns.push_back(std::move(y));
    paramFns.push_back(std::move(theta));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(tgt_wkt));
}

VarVal AlwaysNeTrgeometryGeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto ref_wkt = paramFns[0].execute(record, arena);
    auto x = paramFns[1].execute(record, arena).cast<double>();
    auto y = paramFns[2].execute(record, arena).cast<double>();
    auto theta = paramFns[3].execute(record, arena).cast<double>();
    auto ts = paramFns[4].execute(record, arena).cast<uint64_t>();
    auto tgt_wkt = paramFns[5].execute(record, arena);
    const auto result = nautilus::invoke(
        +[](const char* ref_wkt, uint32_t ref_len, double x, double y, double theta, uint64_t ts, const char* tgt_wkt, uint32_t tgt_len) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* ref_str = (char*)malloc(ref_len + 1);
                memcpy(ref_str, ref_wkt, ref_len); ref_str[ref_len] = '\0';
                GSERIALIZED* gs_ref = geom_in(ref_str, -1); free(ref_str);
                if (!gs_ref) return 0.0;
                Pose* pose = pose_make_2d(x, y, theta, false, 0);
                if (!pose) { free(gs_ref); return 0.0; }
                TInstant* inst = trgeoinst_make(gs_ref, pose, (TimestampTz)ts);
                free(gs_ref); free(pose);
                if (!inst) return 0.0;
                char* tgt_str = (char*)malloc(tgt_len + 1);
                memcpy(tgt_str, tgt_wkt, tgt_len); tgt_str[tgt_len] = '\0';
                GSERIALIZED* gs_tgt = geom_in(tgt_str, -1); free(tgt_str);
                if (!gs_tgt) { free(inst); return 0.0; }
                int r = always_ne_trgeometry_geo((Temporal*)inst, gs_tgt);
                free(inst); free(gs_tgt);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        ref_wkt, x, y, theta, ts, tgt_wkt);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysNeTrgeometryGeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "AlwaysNeTrgeometryGeoPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysNeTrgeometryGeoPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]));
}

} // namespace NES
