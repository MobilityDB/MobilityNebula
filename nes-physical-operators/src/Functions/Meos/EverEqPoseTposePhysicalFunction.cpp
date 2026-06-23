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

#include <Functions/Meos/EverEqPoseTposePhysicalFunction.hpp>
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
}

namespace NES {

EverEqPoseTposePhysicalFunction::EverEqPoseTposePhysicalFunction(PhysicalFunction x2, PhysicalFunction y2, PhysicalFunction theta2, PhysicalFunction x, PhysicalFunction y, PhysicalFunction theta, PhysicalFunction ts)
{
    paramFns.reserve(7);
    paramFns.push_back(std::move(x2));
    paramFns.push_back(std::move(y2));
    paramFns.push_back(std::move(theta2));
    paramFns.push_back(std::move(x));
    paramFns.push_back(std::move(y));
    paramFns.push_back(std::move(theta));
    paramFns.push_back(std::move(ts));
}

VarVal EverEqPoseTposePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto x2 = paramFns[0].execute(record, arena).cast<double>();
    auto y2 = paramFns[1].execute(record, arena).cast<double>();
    auto theta2 = paramFns[2].execute(record, arena).cast<double>();
    auto x = paramFns[3].execute(record, arena).cast<double>();
    auto y = paramFns[4].execute(record, arena).cast<double>();
    auto theta = paramFns[5].execute(record, arena).cast<double>();
    auto ts = paramFns[6].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](double x2, double y2, double theta2, double x, double y, double theta, uint64_t ts) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Pose* pose_s = pose_make_2d(x2, y2, theta2, false, 0);
                if (!pose_s) return 0.0;
                Pose* pose_t = pose_make_2d(x, y, theta, false, 0);
                if (!pose_t) { free(pose_s); return 0.0; }
                Temporal* inst = (Temporal*)tposeinst_make(pose_t, (TimestampTz)ts);
                free(pose_t);
                if (!inst) { free(pose_s); return 0.0; }
                int r = ever_eq_pose_tpose(pose_s, inst);
                free(pose_s); free(inst);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        x2, y2, theta2, x, y, theta, ts);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverEqPoseTposePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 7,
                 "EverEqPoseTposePhysicalFunction requires 7 children but got {}",
                 arguments.childFunctions.size());
    return EverEqPoseTposePhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]),
                                  std::move(arguments.childFunctions[6]));
}

} // namespace NES
