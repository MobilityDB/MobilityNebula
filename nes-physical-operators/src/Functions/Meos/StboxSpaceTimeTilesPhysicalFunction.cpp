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

#include <Functions/Meos/StboxSpaceTimeTilesPhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterStboxSpaceTimeTilesPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

StboxSpaceTimeTilesPhysicalFunction::StboxSpaceTimeTilesPhysicalFunction(PhysicalFunction boxFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function,
                                                          PhysicalFunction arg2Function,
                                                          PhysicalFunction arg3Function,
                                                          PhysicalFunction arg4Function,
                                                          PhysicalFunction arg5Function)
{
    parameterFunctions.reserve(7);
    parameterFunctions.push_back(std::move(boxFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
    parameterFunctions.push_back(std::move(arg2Function));
    parameterFunctions.push_back(std::move(arg3Function));
    parameterFunctions.push_back(std::move(arg4Function));
    parameterFunctions.push_back(std::move(arg5Function));
}

VarVal StboxSpaceTimeTilesPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto box = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<nautilus::val<double>>();
    auto arg1 = parameterValues[2].cast<nautilus::val<double>>();
    auto arg2 = parameterValues[3].cast<nautilus::val<double>>();
    auto arg3 = parameterValues[4].cast<VariableSizedData>();
    auto arg4 = parameterValues[5].cast<VariableSizedData>();
    auto arg5 = parameterValues[6].cast<VariableSizedData>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](const char* boxPtr, uint32_t boxSize,
            double arg0,
            double arg1,
            double arg2,
            const char* arg3Ptr, uint32_t arg3Size,
            const char* arg4Ptr, uint32_t arg4Size,
            const char* arg5Ptr, uint32_t arg5Size) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempS(boxPtr, boxSize);
                while (!tempS.empty() && (tempS.front()=='\'' || tempS.front()=='"')) tempS.erase(tempS.begin());
                while (!tempS.empty() && (tempS.back()=='\'' || tempS.back()=='"')) tempS.pop_back();
                STBox* temp = stbox_in(tempS.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg3S(arg3Ptr, arg3Size);
                while (!arg3S.empty() && (arg3S.front()=='\'' || arg3S.front()=='"')) arg3S.erase(arg3S.begin());
                while (!arg3S.empty() && (arg3S.back()=='\'' || arg3S.back()=='"')) arg3S.pop_back();
                Interval* arg3B = interval_in(arg3S.c_str(), -1);
                if (!arg3B) { free(temp); return (char*) nullptr; }
                std::string arg4S(arg4Ptr, arg4Size);
                while (!arg4S.empty() && (arg4S.front()=='\'' || arg4S.front()=='"')) arg4S.erase(arg4S.begin());
                while (!arg4S.empty() && (arg4S.back()=='\'' || arg4S.back()=='"')) arg4S.pop_back();
                MEOS::Meos::StaticGeometry arg4G(arg4S);
                if (!arg4G.getGeometry()) { free(temp); return (char*) nullptr; }
                std::string arg5S(arg5Ptr, arg5Size);
                while (!arg5S.empty() && (arg5S.front()=='\'' || arg5S.front()=='"')) arg5S.erase(arg5S.begin());
                while (!arg5S.empty() && (arg5S.back()=='\'' || arg5S.back()=='"')) arg5S.pop_back();
                TimestampTz arg5V = timestamptz_in(arg5S.c_str(), -1);

                int _cnt = 0;
                STBox* arr = (STBox*) stbox_space_time_tiles(temp, arg0, arg1, arg2, arg3B, arg4G.getGeometry(), arg5V, true, &_cnt);
                free(temp);
                free(arg3B);
                if (!arr || _cnt <= 0) return (char*) nullptr;
                std::string _s = "{";
                for (int _i = 0; _i < _cnt; _i++) { if (_i) _s += ", "; char* _e = stbox_out(&arr[_i], 15); if (_e) { _s += _e; free(_e); } }
                _s += "}";
                free(arr);
                return strdup(_s.c_str());
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        box.getContent(), box.getContentSize(), arg0, arg1, arg2, arg3.getContent(), arg3.getContentSize(), arg4.getContent(), arg4.getContentSize(), arg5.getContent(), arg5.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterStboxSpaceTimeTilesPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 7,
                 "StboxSpaceTimeTilesPhysicalFunction requires 7 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    auto arg6 = std::move(arguments.childFunctions[6]);
    return StboxSpaceTimeTilesPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6));
}

} // namespace NES
