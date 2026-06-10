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

#include <Functions/Meos/Rgeo/NadTrgeometryTrgeometryPhysicalFunction.hpp>

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
#include <meos_rgeo.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterNadTrgeometryTrgeometryPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

NadTrgeometryTrgeometryPhysicalFunction::NadTrgeometryTrgeometryPhysicalFunction(PhysicalFunction xFunction,
                                                          PhysicalFunction yFunction,
                                                          PhysicalFunction thetaFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction x2Function,
                                                          PhysicalFunction y2Function,
                                                          PhysicalFunction theta2Function,
                                                          PhysicalFunction ts2Function)
{
    parameterFunctions.reserve(8);
    parameterFunctions.push_back(std::move(xFunction));
    parameterFunctions.push_back(std::move(yFunction));
    parameterFunctions.push_back(std::move(thetaFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(x2Function));
    parameterFunctions.push_back(std::move(y2Function));
    parameterFunctions.push_back(std::move(theta2Function));
    parameterFunctions.push_back(std::move(ts2Function));
}

VarVal NadTrgeometryTrgeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
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
    auto x2 = parameterValues[4].cast<nautilus::val<double>>();
    auto y2 = parameterValues[5].cast<nautilus::val<double>>();
    auto theta2 = parameterValues[6].cast<nautilus::val<double>>();
    auto ts2 = parameterValues[7].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double x,
            double y,
            double theta,
            uint64_t ts,
            double x2,
            double y2,
            double theta2,
            uint64_t ts2) -> double {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                GSERIALIZED* tempg = geom_in("Polygon((0 0,1 0,1 1,0 1,0 0))", -1);
                if (!tempg) return 0.0;
                std::string temppw = fmt::format("Pose(Point({} {}),{})@{}", x, y, theta, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temptp = tpose_in(temppw.c_str());
                if (!temptp) { free(tempg); return 0.0; }
                Temporal* temp = geo_tpose_to_trgeometry(tempg, temptp);
                free(tempg); free(temptp);
                if (!temp) return 0.0;
                GSERIALIZED* arg0tg = geom_in("Polygon((0 0,1 0,1 1,0 1,0 0))", -1);
                if (!arg0tg) { free(temp); return 0.0; }
                std::string arg0tpw = fmt::format("Pose(Point({} {}),{})@{}", x2, y2, theta2, MEOS::Meos::convertEpochToTimestamp(ts2));
                Temporal* arg0ttp = tpose_in(arg0tpw.c_str());
                if (!arg0ttp) { free(arg0tg); free(temp); return 0.0; }
                Temporal* arg0t = geo_tpose_to_trgeometry(arg0tg, arg0ttp);
                free(arg0tg); free(arg0ttp);
                if (!arg0t) { free(temp); return 0.0; }

                double r = nad_trgeometry_trgeometry(temp, arg0t);
                free(temp);
                free(arg0t);
                return r;
            }
            catch (const std::exception&)
            {
                return 0.0;
            }
        },
        x, y, theta, ts, x2, y2, theta2, ts2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNadTrgeometryTrgeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 8,
                 "NadTrgeometryTrgeometryPhysicalFunction requires 8 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    auto arg7 = std::move(arguments.childFunctions[7]);
    return NadTrgeometryTrgeometryPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7));
}

} // namespace NES
