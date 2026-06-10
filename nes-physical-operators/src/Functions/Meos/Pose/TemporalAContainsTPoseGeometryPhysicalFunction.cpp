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

#include <Functions/Meos/Pose/TemporalAContainsTPoseGeometryPhysicalFunction.hpp>

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

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTemporalAContainsTPoseGeometryPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TemporalAContainsTPoseGeometryPhysicalFunction::TemporalAContainsTPoseGeometryPhysicalFunction(PhysicalFunction xFunction,
                                                          PhysicalFunction yFunction,
                                                          PhysicalFunction thetaFunction,
                                                          PhysicalFunction timestampFunction,
                                                          PhysicalFunction geometryFunction)
{
    parameterFunctions.reserve(5);
    parameterFunctions.push_back(std::move(xFunction));
    parameterFunctions.push_back(std::move(yFunction));
    parameterFunctions.push_back(std::move(thetaFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(geometryFunction));
}

VarVal TemporalAContainsTPoseGeometryPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto x         = parameterValues[0].cast<nautilus::val<double>>();
    auto y         = parameterValues[1].cast<nautilus::val<double>>();
    auto theta     = parameterValues[2].cast<nautilus::val<double>>();
    auto timestamp = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto geometry  = parameterValues[4].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double xValue, double yValue, double thetaValue, uint64_t timestampValue,
            const char* geometryPtr, uint32_t geometrySize) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(xValue >= -180.0 && xValue <= 180.0 && yValue >= -90.0 && yValue <= 90.0)) return 0;

                const std::string timestampString = MEOS::Meos::convertEpochToTimestamp(timestampValue);
                std::string tposeWkt = fmt::format("Pose(Point({} {}), {})@{}",
                                                  xValue, yValue, thetaValue, timestampString);
                std::string staticGeometryWkt(geometryPtr, geometrySize);

                while (!staticGeometryWkt.empty() && (staticGeometryWkt.front() == '\'' || staticGeometryWkt.front() == '"'))
                    staticGeometryWkt.erase(staticGeometryWkt.begin());
                while (!staticGeometryWkt.empty() && (staticGeometryWkt.back() == '\'' || staticGeometryWkt.back() == '"'))
                    staticGeometryWkt.pop_back();

                if (tposeWkt.empty() || staticGeometryWkt.empty()) return 0;

                Temporal* tpose = tpose_in(tposeWkt.c_str());
                if (!tpose) return 0;
                Temporal* tgeo = tpose_to_tpoint(tpose);
                if (!tgeo) { free(tpose); return 0; }
                MEOS::Meos::StaticGeometry staticGeometry(staticGeometryWkt);
                if (!staticGeometry.getGeometry()) { free(tgeo); free(tpose); return 0; }

                int r = acontains_tgeo_geo(tgeo, staticGeometry.getGeometry());
                free(tgeo);
                free(tpose);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        x, y, theta, timestamp, geometry.getContent(), geometry.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTemporalAContainsTPoseGeometryPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 5,
                 "TemporalAContainsTPoseGeometryPhysicalFunction requires 5 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    return TemporalAContainsTPoseGeometryPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4));
}

} // namespace NES
