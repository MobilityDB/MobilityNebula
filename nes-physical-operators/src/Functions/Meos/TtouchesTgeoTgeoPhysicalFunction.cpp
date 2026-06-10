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

#include <Functions/Meos/TtouchesTgeoTgeoPhysicalFunction.hpp>

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
#include <meos_geo.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTtouchesTgeoTgeoPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TtouchesTgeoTgeoPhysicalFunction::TtouchesTgeoTgeoPhysicalFunction(PhysicalFunction lonFunction,
                                                          PhysicalFunction latFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction lon2Function,
                                                          PhysicalFunction lat2Function,
                                                          PhysicalFunction ts2Function)
{
    parameterFunctions.reserve(6);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(lon2Function));
    parameterFunctions.push_back(std::move(lat2Function));
    parameterFunctions.push_back(std::move(ts2Function));
}

VarVal TtouchesTgeoTgeoPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon = parameterValues[0].cast<nautilus::val<double>>();
    auto lat = parameterValues[1].cast<nautilus::val<double>>();
    auto ts = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto lon2 = parameterValues[3].cast<nautilus::val<double>>();
    auto lat2 = parameterValues[4].cast<nautilus::val<double>>();
    auto ts2 = parameterValues[5].cast<nautilus::val<uint64_t>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](double lon,
            double lat,
            uint64_t ts,
            double lon2,
            double lat2,
            uint64_t ts2) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lon >= -180.0 && lon <= 180.0 && lat >= -90.0 && lat <= 90.0)) return (char*) nullptr;
                std::string tempWkt = fmt::format("SRID=4326;Point({} {})@{}", lon, lat, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tgeompoint_in(tempWkt.c_str());
                if (!temp) return (char*) nullptr;
                if (!(lon2 >= -180.0 && lon2 <= 180.0 && lat2 >= -90.0 && lat2 <= 90.0)) { free(temp); return (char*) nullptr; }
                std::string arg0tW = fmt::format("SRID=4326;Point({} {})@{}", lon2, lat2, MEOS::Meos::convertEpochToTimestamp(ts2));
                Temporal* arg0t = tgeompoint_in(arg0tW.c_str());
                if (!arg0t) { free(temp); return (char*) nullptr; }

                Temporal* res = ttouches_tgeo_tgeo(temp, arg0t);
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
        lon, lat, ts, lon2, lat2, ts2);

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTtouchesTgeoTgeoPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "TtouchesTgeoTgeoPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    return TtouchesTgeoTgeoPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5));
}

} // namespace NES
