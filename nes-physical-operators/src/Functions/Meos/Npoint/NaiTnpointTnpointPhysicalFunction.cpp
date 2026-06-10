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

#include <Functions/Meos/Npoint/NaiTnpointTnpointPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <cstdlib>
#include <cstring>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_npoint.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterNaiTnpointTnpointPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

NaiTnpointTnpointPhysicalFunction::NaiTnpointTnpointPhysicalFunction(PhysicalFunction ridFunction,
                                                          PhysicalFunction fracFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction rid2Function,
                                                          PhysicalFunction frac2Function,
                                                          PhysicalFunction ts2Function)
{
    parameterFunctions.reserve(6);
    parameterFunctions.push_back(std::move(ridFunction));
    parameterFunctions.push_back(std::move(fracFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(rid2Function));
    parameterFunctions.push_back(std::move(frac2Function));
    parameterFunctions.push_back(std::move(ts2Function));
}

VarVal NaiTnpointTnpointPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
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
    auto rid2 = parameterValues[3].cast<nautilus::val<int64_t>>();
    auto frac2 = parameterValues[4].cast<nautilus::val<double>>();
    auto ts2 = parameterValues[5].cast<nautilus::val<uint64_t>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](int64_t rid,
            double frac,
            uint64_t ts,
            int64_t rid2,
            double frac2,
            uint64_t ts2) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (frac < 0.0 || frac > 1.0) return (char*) nullptr;
                std::string tempWkt = fmt::format("NPoint({},{})@{}", rid, frac, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tnpoint_in(tempWkt.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg0tW = fmt::format("NPoint({},{})@{}", rid2, frac2, MEOS::Meos::convertEpochToTimestamp(ts2));
                Temporal* arg0t = tnpoint_in(arg0tW.c_str());
                if (!arg0t) { free(temp); return (char*) nullptr; }

                Temporal* res = (Temporal*) nai_tnpoint_tnpoint(temp, arg0t);
                free(temp);
                free(arg0t);
                if (!res) return (char*) nullptr;
                char* outStr = tspatial_as_text(res, 15);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        rid, frac, ts, rid2, frac2, ts2);

    const auto outLen = nautilus::invoke(
        +[](const char* s) -> uint32_t { return s ? (uint32_t) strlen(s) : (uint32_t) 0; },
        outStr);

    auto variableSized = arena.allocateVariableSizedData(outLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, uint32_t len) -> void
        {
            if (s)
            {
                memcpy(dest, s, len);
                free((void*) s);
            }
        },
        variableSized.getContent(), outStr, outLen);

    return variableSized;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterNaiTnpointTnpointPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "NaiTnpointTnpointPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    return NaiTnpointTnpointPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5));
}

} // namespace NES
