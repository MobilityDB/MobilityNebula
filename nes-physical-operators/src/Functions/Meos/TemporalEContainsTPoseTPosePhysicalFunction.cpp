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

#include <Functions/Meos/TemporalEContainsTPoseTPosePhysicalFunction.hpp>

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
#include <meos_geo.h>
#include <meos_pose.h>
}

namespace NES {

TemporalEContainsTPoseTPosePhysicalFunction::TemporalEContainsTPoseTPosePhysicalFunction(PhysicalFunction xAFunction,
                                                          PhysicalFunction yAFunction,
                                                          PhysicalFunction thetaAFunction,
                                                          PhysicalFunction tsAFunction,
                                                          PhysicalFunction xBFunction,
                                                          PhysicalFunction yBFunction,
                                                          PhysicalFunction thetaBFunction,
                                                          PhysicalFunction tsBFunction)
{
    parameterFunctions.reserve(8);
    parameterFunctions.push_back(std::move(xAFunction));
    parameterFunctions.push_back(std::move(yAFunction));
    parameterFunctions.push_back(std::move(thetaAFunction));
    parameterFunctions.push_back(std::move(tsAFunction));
    parameterFunctions.push_back(std::move(xBFunction));
    parameterFunctions.push_back(std::move(yBFunction));
    parameterFunctions.push_back(std::move(thetaBFunction));
    parameterFunctions.push_back(std::move(tsBFunction));
}

VarVal TemporalEContainsTPoseTPosePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto xA     = parameterValues[0].cast<nautilus::val<double>>();
    auto yA     = parameterValues[1].cast<nautilus::val<double>>();
    auto thetaA = parameterValues[2].cast<nautilus::val<double>>();
    auto tsA    = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto xB     = parameterValues[4].cast<nautilus::val<double>>();
    auto yB     = parameterValues[5].cast<nautilus::val<double>>();
    auto thetaB = parameterValues[6].cast<nautilus::val<double>>();
    auto tsB    = parameterValues[7].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double xAValue, double yAValue, double thetaAValue, uint64_t tsAValue,
            double xBValue, double yBValue, double thetaBValue, uint64_t tsBValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(xAValue >= -180.0 && xAValue <= 180.0 && yAValue >= -90.0 && yAValue <= 90.0)) return 0;
                if (!(xBValue >= -180.0 && xBValue <= 180.0 && yBValue >= -90.0 && yBValue <= 90.0)) return 0;

                const std::string tsAString = MEOS::Meos::convertEpochToTimestamp(tsAValue);
                const std::string tsBString = MEOS::Meos::convertEpochToTimestamp(tsBValue);
                std::string tposeAWkt = fmt::format("Pose(Point({} {}), {})@{}", xAValue, yAValue, thetaAValue, tsAString);
                std::string tposeBWkt = fmt::format("Pose(Point({} {}), {})@{}", xBValue, yBValue, thetaBValue, tsBString);

                if (tposeAWkt.empty() || tposeBWkt.empty()) return 0;

                Temporal* tposeA = tpose_in(tposeAWkt.c_str());
                if (!tposeA) return 0;
                Temporal* tgeoA = tpose_to_tpoint(tposeA);
                if (!tgeoA) { free(tposeA); return 0; }
                Temporal* tposeB = tpose_in(tposeBWkt.c_str());
                if (!tposeB) { free(tgeoA); free(tposeA); return 0; }
                Temporal* tgeoB = tpose_to_tpoint(tposeB);
                if (!tgeoB) { free(tposeB); free(tgeoA); free(tposeA); return 0; }

                int r = econtains_tgeo_tgeo(tgeoA, tgeoB);
                free(tgeoB);
                free(tposeB);
                free(tgeoA);
                free(tposeA);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        xA, yA, thetaA, tsA, xB, yB, thetaB, tsB);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTemporalEContainsTPoseTPosePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 8,
                 "TemporalEContainsTPoseTPosePhysicalFunction requires 8 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    auto arg7 = std::move(arguments.childFunctions[7]);
    return TemporalEContainsTPoseTPosePhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7));
}

} // namespace NES
