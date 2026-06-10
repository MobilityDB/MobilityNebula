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

#include <Functions/Meos/TpointAtElevationPhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTpointAtElevationPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TpointAtElevationPhysicalFunction::TpointAtElevationPhysicalFunction(PhysicalFunction trajFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(2);
    parameterFunctions.push_back(std::move(trajFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal TpointAtElevationPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto traj = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<VariableSizedData>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](const char* trajPtr, uint32_t trajSize,
            const char* arg0Ptr, uint32_t arg0Size) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempHex(trajPtr, trajSize);
                Temporal* temp = temporal_from_hexwkb(tempHex.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                Span* arg0B = floatspan_in(arg0S.c_str());
                if (!arg0B) { free(temp); return (char*) nullptr; }

                Temporal* res = (Temporal*) tpoint_at_elevation(temp, arg0B);
                free(temp);
                free(arg0B);
                if (!res) return (char*) nullptr;
                size_t _wsz = 0;
                char* outStr = temporal_as_hexwkb(res, 0x04 /* WKB_EXTENDED */, &_wsz);
                free(res);
                if (!outStr) return (char*) nullptr;
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        traj.getContent(), traj.getContentSize(), arg0.getContent(), arg0.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTpointAtElevationPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "TpointAtElevationPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    return TpointAtElevationPhysicalFunction(std::move(arg0), std::move(arg1));
}

} // namespace NES
