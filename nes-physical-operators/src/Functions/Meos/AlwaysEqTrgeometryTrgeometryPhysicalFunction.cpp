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

#include <Functions/Meos/AlwaysEqTrgeometryTrgeometryPhysicalFunction.hpp>
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

AlwaysEqTrgeometryTrgeometryPhysicalFunction::AlwaysEqTrgeometryTrgeometryPhysicalFunction(PhysicalFunction ref1_wkt, PhysicalFunction x1, PhysicalFunction y1, PhysicalFunction theta1, PhysicalFunction ts1, PhysicalFunction ref2_wkt, PhysicalFunction x2, PhysicalFunction y2, PhysicalFunction theta2, PhysicalFunction ts2)
{
    paramFns.reserve(10);
    paramFns.push_back(std::move(ref1_wkt));
    paramFns.push_back(std::move(x1));
    paramFns.push_back(std::move(y1));
    paramFns.push_back(std::move(theta1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(ref2_wkt));
    paramFns.push_back(std::move(x2));
    paramFns.push_back(std::move(y2));
    paramFns.push_back(std::move(theta2));
    paramFns.push_back(std::move(ts2));
}

VarVal AlwaysEqTrgeometryTrgeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto ref1_wkt = paramFns[0].execute(record, arena);
    auto x1 = paramFns[1].execute(record, arena).cast<double>();
    auto y1 = paramFns[2].execute(record, arena).cast<double>();
    auto theta1 = paramFns[3].execute(record, arena).cast<double>();
    auto ts1 = paramFns[4].execute(record, arena).cast<uint64_t>();
    auto ref2_wkt = paramFns[5].execute(record, arena);
    auto x2 = paramFns[6].execute(record, arena).cast<double>();
    auto y2 = paramFns[7].execute(record, arena).cast<double>();
    auto theta2 = paramFns[8].execute(record, arena).cast<double>();
    auto ts2 = paramFns[9].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](const char* ref1_wkt, uint32_t ref1_len, double x1, double y1, double theta1, uint64_t ts1, const char* ref2_wkt, uint32_t ref2_len, double x2, double y2, double theta2, uint64_t ts2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* s1 = (char*)malloc(ref1_len + 1);
                memcpy(s1, ref1_wkt, ref1_len); s1[ref1_len] = '\0';
                GSERIALIZED* gs1 = geom_in(s1, -1); free(s1);
                if (!gs1) return 0.0;
                Pose* p1 = pose_make_2d(x1, y1, theta1, false, 0);
                if (!p1) { free(gs1); return 0.0; }
                TInstant* inst1 = trgeoinst_make(gs1, p1, (TimestampTz)ts1);
                free(gs1); free(p1);
                if (!inst1) return 0.0;
                char* s2 = (char*)malloc(ref2_len + 1);
                memcpy(s2, ref2_wkt, ref2_len); s2[ref2_len] = '\0';
                GSERIALIZED* gs2 = geom_in(s2, -1); free(s2);
                if (!gs2) { free(inst1); return 0.0; }
                Pose* p2 = pose_make_2d(x2, y2, theta2, false, 0);
                if (!p2) { free(inst1); free(gs2); return 0.0; }
                TInstant* inst2 = trgeoinst_make(gs2, p2, (TimestampTz)ts2);
                free(gs2); free(p2);
                if (!inst2) { free(inst1); return 0.0; }
                int r = always_eq_trgeometry_trgeometry((Temporal*)inst1, (Temporal*)inst2);
                free(inst1); free(inst2);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        ref1_wkt, x1, y1, theta1, ts1, ref2_wkt, x2, y2, theta2, ts2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysEqTrgeometryTrgeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 10,
                 "AlwaysEqTrgeometryTrgeometryPhysicalFunction requires 10 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysEqTrgeometryTrgeometryPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]),
                                  std::move(arguments.childFunctions[6]),
                                  std::move(arguments.childFunctions[7]),
                                  std::move(arguments.childFunctions[8]),
                                  std::move(arguments.childFunctions[9]));
}

} // namespace NES
