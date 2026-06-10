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

#include <Functions/Meos/StboxMakePhysicalFunction.hpp>

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
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterStboxMakePhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

StboxMakePhysicalFunction::StboxMakePhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function,
                                                          PhysicalFunction arg2Function,
                                                          PhysicalFunction arg3Function,
                                                          PhysicalFunction arg4Function,
                                                          PhysicalFunction arg5Function,
                                                          PhysicalFunction arg6Function,
                                                          PhysicalFunction arg7Function,
                                                          PhysicalFunction arg8Function,
                                                          PhysicalFunction arg9Function)
{
    parameterFunctions.reserve(11);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
    parameterFunctions.push_back(std::move(arg2Function));
    parameterFunctions.push_back(std::move(arg3Function));
    parameterFunctions.push_back(std::move(arg4Function));
    parameterFunctions.push_back(std::move(arg5Function));
    parameterFunctions.push_back(std::move(arg6Function));
    parameterFunctions.push_back(std::move(arg7Function));
    parameterFunctions.push_back(std::move(arg8Function));
    parameterFunctions.push_back(std::move(arg9Function));
}

VarVal StboxMakePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<nautilus::val<bool>>();
    auto arg0 = parameterValues[1].cast<nautilus::val<bool>>();
    auto arg1 = parameterValues[2].cast<nautilus::val<bool>>();
    auto arg2 = parameterValues[3].cast<nautilus::val<int32_t>>();
    auto arg3 = parameterValues[4].cast<nautilus::val<double>>();
    auto arg4 = parameterValues[5].cast<nautilus::val<double>>();
    auto arg5 = parameterValues[6].cast<nautilus::val<double>>();
    auto arg6 = parameterValues[7].cast<nautilus::val<double>>();
    auto arg7 = parameterValues[8].cast<nautilus::val<double>>();
    auto arg8 = parameterValues[9].cast<nautilus::val<double>>();
    auto arg9 = parameterValues[10].cast<VariableSizedData>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](bool value,
            bool arg0,
            bool arg1,
            int32_t arg2,
            double arg3,
            double arg4,
            double arg5,
            double arg6,
            double arg7,
            double arg8,
            const char* arg9Ptr, uint32_t arg9Size) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                bool temp = (bool) value;
                std::string arg9S(arg9Ptr, arg9Size);
                while (!arg9S.empty() && (arg9S.front()=='\'' || arg9S.front()=='"')) arg9S.erase(arg9S.begin());
                while (!arg9S.empty() && (arg9S.back()=='\'' || arg9S.back()=='"')) arg9S.pop_back();
                Span* arg9B = tstzspan_in(arg9S.c_str());
                if (!arg9B) { return (char*) nullptr; }

                STBox* res = (STBox*) stbox_make(temp, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9B);
                free(arg9B);
                if (!res) return (char*) nullptr;
                char* outStr = stbox_out(res, 15);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        value, arg0, arg1, arg2, arg3, arg4, arg5, arg6, arg7, arg8, arg9.getContent(), arg9.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterStboxMakePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 11,
                 "StboxMakePhysicalFunction requires 11 children but got {}",
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
    auto arg9 = std::move(arguments.childFunctions[9]);
    auto arg10 = std::move(arguments.childFunctions[10]);
    return StboxMakePhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5), std::move(arg6), std::move(arg7), std::move(arg8), std::move(arg9), std::move(arg10));
}

} // namespace NES
