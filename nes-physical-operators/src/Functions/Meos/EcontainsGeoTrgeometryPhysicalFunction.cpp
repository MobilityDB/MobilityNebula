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

#include <Functions/Meos/EcontainsGeoTrgeometryPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
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

EcontainsGeoTrgeometryPhysicalFunction::EcontainsGeoTrgeometryPhysicalFunction(PhysicalFunction tgt_wkt, PhysicalFunction ref_wkt, PhysicalFunction x1, PhysicalFunction y1, PhysicalFunction theta1, PhysicalFunction ts1) {
    paramFns.reserve(6);
    paramFns.push_back(std::move(tgt_wkt));
    paramFns.push_back(std::move(ref_wkt));
    paramFns.push_back(std::move(x1));
    paramFns.push_back(std::move(y1));
    paramFns.push_back(std::move(theta1));
    paramFns.push_back(std::move(ts1));
}

VarVal EcontainsGeoTrgeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto tgt_wkt = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto ref_wkt = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    auto x1 = paramFns[2].execute(record, arena).cast<double>();
    auto y1 = paramFns[3].execute(record, arena).cast<double>();
    auto theta1 = paramFns[4].execute(record, arena).cast<double>();
    auto ts1 = paramFns[5].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](const char* tgt_wkt, uint32_t tgt_wktsz, const char* ref_wkt, uint32_t ref_wktsz, double x1, double y1, double theta1, uint64_t ts1) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* ref1_str = (char*)malloc(ref_wktsz + 1);
                memcpy(ref1_str, ref_wkt, ref_wktsz); ref1_str[ref_wktsz] = '\0';
                GSERIALIZED* gref1 = geom_in(ref1_str, -1); free(ref1_str);
                if (!gref1) return 0.0;
                Pose* pose1 = pose_make_2d(x1, y1, theta1, false, 0);
                if (!pose1) { free(gref1); return 0.0; }
                TInstant* inst1 = trgeometryinst_make(gref1, pose1, (TimestampTz)ts1);
                free(gref1); free(pose1);
                if (!inst1) return 0.0;
                char* tgt_str = (char*)malloc(tgt_wktsz + 1);
                memcpy(tgt_str, tgt_wkt, tgt_wktsz); tgt_str[tgt_wktsz] = '\0';
                GSERIALIZED* gs_tgt = geom_in(tgt_str, -1); free(tgt_str);
                if (!gs_tgt) { free(inst1); return 0.0; }
                int r = econtains_geo_trgeometry(gs_tgt, (Temporal*)inst1);
                free(inst1); free(gs_tgt);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        tgt_wkt, ref_wkt, x1, y1, theta1, ts1);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEcontainsGeoTrgeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==6,
                 "EcontainsGeoTrgeometryPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    return EcontainsGeoTrgeometryPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]));
}

} // namespace NES
