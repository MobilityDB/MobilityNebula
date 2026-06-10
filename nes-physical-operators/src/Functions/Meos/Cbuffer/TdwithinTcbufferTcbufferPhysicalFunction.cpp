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

#include <Functions/Meos/Cbuffer/TdwithinTcbufferTcbufferPhysicalFunction.hpp>

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
#include <meos_cbuffer.h>
#include <meos_geo.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTdwithinTcbufferTcbufferPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TdwithinTcbufferTcbufferPhysicalFunction::TdwithinTcbufferTcbufferPhysicalFunction(PhysicalFunction lonFunction,
                                                          PhysicalFunction latFunction,
                                                          PhysicalFunction radiusFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction lon2Function,
                                                          PhysicalFunction lat2Function,
                                                          PhysicalFunction radius2Function,
                                                          PhysicalFunction ts2Function,
                                                          PhysicalFunction arg1Function)
{
    parameterFunctions.reserve(9);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(radiusFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(lon2Function));
    parameterFunctions.push_back(std::move(lat2Function));
    parameterFunctions.push_back(std::move(radius2Function));
    parameterFunctions.push_back(std::move(ts2Function));
    parameterFunctions.push_back(std::move(arg1Function));
}

VarVal TdwithinTcbufferTcbufferPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon = parameterValues[0].cast<nautilus::val<double>>();
    auto lat = parameterValues[1].cast<nautilus::val<double>>();
    auto radius = parameterValues[2].cast<nautilus::val<double>>();
    auto ts = parameterValues[3].cast<nautilus::val<uint64_t>>();
    auto lon2 = parameterValues[4].cast<nautilus::val<double>>();
    auto lat2 = parameterValues[5].cast<nautilus::val<double>>();
    auto radius2 = parameterValues[6].cast<nautilus::val<double>>();
    auto ts2 = parameterValues[7].cast<nautilus::val<uint64_t>>();
    auto arg1 = parameterValues[8].cast<nautilus::val<double>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](double lon,
            double lat,
            double radius,
            uint64_t ts,
            double lon2,
            double lat2,
            double radius2,
            uint64_t ts2,
            double arg1) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lon >= -180.0 && lon <= 180.0 && lat >= -90.0 && lat <= 90.0) || radius < 0.0) return (char*) nullptr;
                std::string tempWkt = fmt::format("Cbuffer(Point({} {}),{})@{}", lon, lat, radius, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tcbuffer_in(tempWkt.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg0tW = fmt::format("Cbuffer(Point({} {}),{})@{}", lon2, lat2, radius2, MEOS::Meos::convertEpochToTimestamp(ts2));
                Temporal* arg0t = tcbuffer_in(arg0tW.c_str());
                if (!arg0t) { free(temp); return (char*) nullptr; }

                Temporal* res = (Temporal*) tdwithin_tcbuffer_tcbuffer(temp, arg0t, arg1);
                free(temp);
                free(arg0t);
                if (!res) return (char*) nullptr;
                char* outStr = tbool_out(res);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        lon, lat, radius, ts, lon2, lat2, radius2, ts2, arg1);

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTdwithinTcbufferTcbufferPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 9,
                 "TdwithinTcbufferTcbufferPhysicalFunction requires 9 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    auto arg7 = std::move(arguments.childFunctions[7]);
    auto arg8 = std::move(arguments.childFunctions[8]);
    return TdwithinTcbufferTcbufferPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8));
}

} // namespace NES
