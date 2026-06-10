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

#include <Functions/Meos/Npoint/NadTnpointNpointPhysicalFunction.hpp>

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
#include <meos_npoint.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterNadTnpointNpointPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

NadTnpointNpointPhysicalFunction::NadTnpointNpointPhysicalFunction(PhysicalFunction ridFunction,
                                                          PhysicalFunction fracFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(ridFunction));
    parameterFunctions.push_back(std::move(fracFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal NadTnpointNpointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto rid = parameterValues[0].cast<nautilus::val<int64_t>>();
    auto frac = parameterValues[1].cast<nautilus::val<double>>();
    auto ts = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto arg0 = parameterValues[3].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](int64_t rid,
            double frac,
            uint64_t ts,
            const char* arg0Ptr, uint32_t arg0Size) -> double {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (frac < 0.0 || frac > 1.0) return 0.0;
                std::string tempWkt = fmt::format("NPoint({},{})@{}", rid, frac, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tnpoint_in(tempWkt.c_str());
                if (!temp) return 0.0;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                Npoint* arg0B = npoint_in(arg0S.c_str());
                if (!arg0B) { free(temp); return 0.0; }

                double r = nad_tnpoint_npoint(temp, arg0B);
                free(temp);
                free(arg0B);
                return r;
            }
            catch (const std::exception&)
            {
                return 0.0;
            }
        },
        rid, frac, ts, arg0.getContent(), arg0.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNadTnpointNpointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "NadTnpointNpointPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    return NadTnpointNpointPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
