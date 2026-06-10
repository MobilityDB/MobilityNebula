#define NES_PLUGIN_OPERATOR_TU
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

#include <Functions/Meos/Pose/AlwaysEqTposePosePhysicalFunction.hpp>

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
#include <meos_pose.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterAlwaysEqTposePosePhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

AlwaysEqTposePosePhysicalFunction::AlwaysEqTposePosePhysicalFunction(PhysicalFunction xFunction,
                                                          PhysicalFunction yFunction,
                                                          PhysicalFunction thetaFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(xFunction));
    parameterFunctions.push_back(std::move(yFunction));
    parameterFunctions.push_back(std::move(thetaFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal AlwaysEqTposePosePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto x = parameterValues[0].cast<nautilus::val<double>>();
    auto y = parameterValues[1].cast<nautilus::val<double>>();
    auto theta = parameterValues[2].cast<nautilus::val<double>>();
    auto ts = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto arg0 = parameterValues[4].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double x,
            double y,
            double theta,
            uint64_t ts,
            const char* arg0Ptr, uint32_t arg0Size) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempWkt = fmt::format("Pose(Point({} {}),{})@{}", x, y, theta, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tpose_in(tempWkt.c_str());
                if (!temp) return 0;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                Pose* arg0B = pose_in(arg0S.c_str());
                if (!arg0B) { free(temp); return 0; }

                int r = always_eq_tpose_pose(temp, arg0B);
                free(temp);
                free(arg0B);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        x, y, theta, ts, arg0.getContent(), arg0.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysEqTposePosePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "AlwaysEqTposePosePhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    return AlwaysEqTposePosePhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4));
}

} // namespace NES
