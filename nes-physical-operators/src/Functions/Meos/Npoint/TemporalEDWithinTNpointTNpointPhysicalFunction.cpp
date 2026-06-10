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

#include <Functions/Meos/Npoint/TemporalEDWithinTNpointTNpointPhysicalFunction.hpp>

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
#include <meos_npoint.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTemporalEDWithinTNpointTNpointPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TemporalEDWithinTNpointTNpointPhysicalFunction::TemporalEDWithinTNpointTNpointPhysicalFunction(PhysicalFunction ridAFunction,
                                                          PhysicalFunction fractionAFunction,
                                                          PhysicalFunction tsAFunction,
                                                          PhysicalFunction ridBFunction,
                                                          PhysicalFunction fractionBFunction,
                                                          PhysicalFunction tsBFunction,
                                                          PhysicalFunction distFunction)
{
    parameterFunctions.reserve(7);
    parameterFunctions.push_back(std::move(ridAFunction));
    parameterFunctions.push_back(std::move(fractionAFunction));
    parameterFunctions.push_back(std::move(tsAFunction));
    parameterFunctions.push_back(std::move(ridBFunction));
    parameterFunctions.push_back(std::move(fractionBFunction));
    parameterFunctions.push_back(std::move(tsBFunction));
    parameterFunctions.push_back(std::move(distFunction));
}

VarVal TemporalEDWithinTNpointTNpointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto ridA      = parameterValues[0].cast<nautilus::val<uint64_t>>();
    auto fractionA = parameterValues[1].cast<nautilus::val<double>>();
    auto tsA       = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto ridB      = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto fractionB = parameterValues[4].cast<nautilus::val<double>>();
    auto tsB       = parameterValues[5].cast<nautilus::val<uint64_t>>();
    auto dist      = parameterValues[6].cast<nautilus::val<double>>();

    const auto result = nautilus::invoke(
        +[](uint64_t ridAValue, double fractionAValue, uint64_t tsAValue,
            uint64_t ridBValue, double fractionBValue, uint64_t tsBValue,
            double distValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();

                const std::string tsAString = MEOS::Meos::convertEpochToTimestamp(tsAValue);
                const std::string tsBString = MEOS::Meos::convertEpochToTimestamp(tsBValue);
                std::string tnpointAWkt = fmt::format("NPoint({}, {})@{}", ridAValue, fractionAValue, tsAString);
                std::string tnpointBWkt = fmt::format("NPoint({}, {})@{}", ridBValue, fractionBValue, tsBString);

                if (tnpointAWkt.empty() || tnpointBWkt.empty()) return 0;

                Temporal* tnpointA = tnpoint_in(tnpointAWkt.c_str());
                if (!tnpointA) return 0;
                Temporal* tgeoA = tnpoint_to_tgeompoint(tnpointA);
                if (!tgeoA) { free(tnpointA); return 0; }
                Temporal* tnpointB = tnpoint_in(tnpointBWkt.c_str());
                if (!tnpointB) { free(tgeoA); free(tnpointA); return 0; }
                Temporal* tgeoB = tnpoint_to_tgeompoint(tnpointB);
                if (!tgeoB) { free(tnpointB); free(tgeoA); free(tnpointA); return 0; }

                int r = edwithin_tgeo_tgeo(tgeoA, tgeoB, distValue);
                free(tgeoB);
                free(tnpointB);
                free(tgeoA);
                free(tnpointA);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        ridA, fractionA, tsA, ridB, fractionB, tsB, dist);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTemporalEDWithinTNpointTNpointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 7,
                 "TemporalEDWithinTNpointTNpointPhysicalFunction requires 7 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    return TemporalEDWithinTNpointTNpointPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6));
}

} // namespace NES
