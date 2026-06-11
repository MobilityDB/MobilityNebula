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

#include <Functions/Meos/AlwaysGtTemporalTemporalPhysicalFunction.hpp>

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
}

namespace NES {

AlwaysGtTemporalTemporalPhysicalFunction::AlwaysGtTemporalTemporalPhysicalFunction(PhysicalFunction valueAFunction,
                                                          PhysicalFunction tsAFunction,
                                                          PhysicalFunction valueBFunction,
                                                          PhysicalFunction tsBFunction)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(valueAFunction));
    parameterFunctions.push_back(std::move(tsAFunction));
    parameterFunctions.push_back(std::move(valueBFunction));
    parameterFunctions.push_back(std::move(tsBFunction));
}

VarVal AlwaysGtTemporalTemporalPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto valueA = parameterValues[0].cast<nautilus::val<double>>();
    auto tsA    = parameterValues[1].cast<nautilus::val<uint64_t>>();
    auto valueB = parameterValues[2].cast<nautilus::val<double>>();
    auto tsB    = parameterValues[3].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double valueAValue, uint64_t tsAValue,
            double valueBValue, uint64_t tsBValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                const std::string tsAStr = MEOS::Meos::convertEpochToTimestamp(tsAValue);
                const std::string tsBStr = MEOS::Meos::convertEpochToTimestamp(tsBValue);
                std::string wktA = fmt::format("{}@{}", valueAValue, tsAStr);
                std::string wktB = fmt::format("{}@{}", valueBValue, tsBStr);
                Temporal* tempA = tfloat_in(wktA.c_str());
                if (!tempA) return 0;
                Temporal* tempB = tfloat_in(wktB.c_str());
                if (!tempB) { free(tempA); return 0; }
                int r = always_gt_temporal_temporal(tempA, tempB);
                free(tempA);
                free(tempB);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        valueA, tsA, valueB, tsB);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysGtTemporalTemporalPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "AlwaysGtTemporalTemporalPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    return AlwaysGtTemporalTemporalPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
