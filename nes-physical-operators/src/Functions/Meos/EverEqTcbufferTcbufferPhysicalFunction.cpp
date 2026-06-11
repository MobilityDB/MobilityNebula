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

#include <Functions/Meos/EverEqTcbufferTcbufferPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
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
#include <meos_cbuffer.h>
}

namespace NES {

EverEqTcbufferTcbufferPhysicalFunction::EverEqTcbufferTcbufferPhysicalFunction(PhysicalFunction lonAFunction,
                                                          PhysicalFunction latAFunction,
                                                          PhysicalFunction radiusAFunction,
                                                          PhysicalFunction tsAFunction,
                                                          PhysicalFunction lonBFunction,
                                                          PhysicalFunction latBFunction,
                                                          PhysicalFunction radiusBFunction,
                                                          PhysicalFunction tsBFunction)
{
    parameterFunctions.reserve(8);
    parameterFunctions.push_back(std::move(lonAFunction));
    parameterFunctions.push_back(std::move(latAFunction));
    parameterFunctions.push_back(std::move(radiusAFunction));
    parameterFunctions.push_back(std::move(tsAFunction));
    parameterFunctions.push_back(std::move(lonBFunction));
    parameterFunctions.push_back(std::move(latBFunction));
    parameterFunctions.push_back(std::move(radiusBFunction));
    parameterFunctions.push_back(std::move(tsBFunction));
}

VarVal EverEqTcbufferTcbufferPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lonA    = parameterValues[0].cast<nautilus::val<double>>();
    auto latA    = parameterValues[1].cast<nautilus::val<double>>();
    auto radiusA = parameterValues[2].cast<nautilus::val<double>>();
    auto tsA     = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto lonB    = parameterValues[4].cast<nautilus::val<double>>();
    auto latB    = parameterValues[5].cast<nautilus::val<double>>();
    auto radiusB = parameterValues[6].cast<nautilus::val<double>>();
    auto tsB     = parameterValues[7].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double lonAValue, double latAValue, double radiusAValue, uint64_t tsAValue,
            double lonBValue, double latBValue, double radiusBValue, uint64_t tsBValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lonAValue >= -180.0 && lonAValue <= 180.0 && latAValue >= -90.0 && latAValue <= 90.0)) return 0;
                if (!(lonBValue >= -180.0 && lonBValue <= 180.0 && latBValue >= -90.0 && latBValue <= 90.0)) return 0;
                if (radiusAValue < 0.0 || radiusBValue < 0.0) return 0;

                const std::string tsAString = MEOS::Meos::convertEpochToTimestamp(tsAValue);
                const std::string tsBString = MEOS::Meos::convertEpochToTimestamp(tsBValue);
                std::string wktA = fmt::format("Cbuffer(Point({} {}),{})@{}", lonAValue, latAValue, radiusAValue, tsAString);
                std::string wktB = fmt::format("Cbuffer(Point({} {}),{})@{}", lonBValue, latBValue, radiusBValue, tsBString);

                Temporal* tA = tcbuffer_in(wktA.c_str());
                if (!tA) return 0;
                Temporal* tB = tcbuffer_in(wktB.c_str());
                if (!tB) { free(tA); return 0; }

                int r = ever_eq_tcbuffer_tcbuffer(tA, tB);
                free(tA);
                free(tB);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        lonA, latA, radiusA, tsA, lonB, latB, radiusB, tsB);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverEqTcbufferTcbufferPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 8,
                 "EverEqTcbufferTcbufferPhysicalFunction requires 8 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    auto arg7 = std::move(arguments.childFunctions[7]);
    return EverEqTcbufferTcbufferPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7));
}

} // namespace NES
