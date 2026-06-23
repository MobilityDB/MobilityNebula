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

#include <Functions/Meos/NadTposePosePhysicalFunction.hpp>
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

NadTposePosePhysicalFunction::NadTposePosePhysicalFunction(PhysicalFunction x, PhysicalFunction y, PhysicalFunction theta, PhysicalFunction ts, PhysicalFunction x2, PhysicalFunction y2, PhysicalFunction theta2)
{
    paramFns.reserve(7);
    paramFns.push_back(std::move(x));
    paramFns.push_back(std::move(y));
    paramFns.push_back(std::move(theta));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(x2));
    paramFns.push_back(std::move(y2));
    paramFns.push_back(std::move(theta2));
}

VarVal NadTposePosePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto x = paramFns[0].execute(record, arena).cast<double>();
    auto y = paramFns[1].execute(record, arena).cast<double>();
    auto theta = paramFns[2].execute(record, arena).cast<double>();
    auto ts = paramFns[3].execute(record, arena).cast<uint64_t>();
    auto x2 = paramFns[4].execute(record, arena).cast<double>();
    auto y2 = paramFns[5].execute(record, arena).cast<double>();
    auto theta2 = paramFns[6].execute(record, arena).cast<double>();
    const auto result = nautilus::invoke(
        +[](double x, double y, double theta, uint64_t ts, double x2, double y2, double theta2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                Pose* pose1 = pose_make_2d(x, y, theta, false, 0);
                if (!pose1) return 0.0;
                Temporal* inst = (Temporal*)tposeinst_make(pose1, (TimestampTz)ts);
                free(pose1);
                if (!inst) return 0.0;
                Pose* pose2 = pose_make_2d(x2, y2, theta2, false, 0);
                if (!pose2) { free(inst); return 0.0; }
                double r = nad_tpose_pose(inst, pose2);
                free(inst); free(pose2);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        x, y, theta, ts, x2, y2, theta2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNadTposePosePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 7,
                 "NadTposePosePhysicalFunction requires 7 children but got {}",
                 arguments.childFunctions.size());
    return NadTposePosePhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]),
                                  std::move(arguments.childFunctions[4]),
                                  std::move(arguments.childFunctions[5]),
                                  std::move(arguments.childFunctions[6]));
}

} // namespace NES
