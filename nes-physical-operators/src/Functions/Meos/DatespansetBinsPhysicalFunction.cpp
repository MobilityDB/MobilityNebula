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

#include <Functions/Meos/DatespansetBinsPhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterDatespansetBinsPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

DatespansetBinsPhysicalFunction::DatespansetBinsPhysicalFunction(PhysicalFunction litFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(litFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
}

VarVal DatespansetBinsPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lit = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<VariableSizedData>();
    auto arg1 = parameterValues[2].cast<VariableSizedData>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](const char* litPtr, uint32_t litSize,
            const char* arg0Ptr, uint32_t arg0Size,
            const char* arg1Ptr, uint32_t arg1Size) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempS(litPtr, litSize);
                while (!tempS.empty() && (tempS.front()=='\'' || tempS.front()=='"')) tempS.erase(tempS.begin());
                while (!tempS.empty() && (tempS.back()=='\'' || tempS.back()=='"')) tempS.pop_back();
                SpanSet* temp = datespanset_in(tempS.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                Interval* arg0B = interval_in(arg0S.c_str(), -1);
                if (!arg0B) { free(temp); return (char*) nullptr; }
                std::string arg1S(arg1Ptr, arg1Size);
                while (!arg1S.empty() && (arg1S.front()=='\'' || arg1S.front()=='"')) arg1S.erase(arg1S.begin());
                while (!arg1S.empty() && (arg1S.back()=='\'' || arg1S.back()=='"')) arg1S.pop_back();
                DateADT arg1V = date_in(arg1S.c_str());

                int _cnt = 0;
                Span* arr = (Span*) datespanset_bins(temp, arg0B, arg1V, &_cnt);
                free(temp);
                free(arg0B);
                if (!arr || _cnt <= 0) return (char*) nullptr;
                std::string _s = "{";
                for (int _i = 0; _i < _cnt; _i++) { if (_i) _s += ", "; char* _e = datespan_out(&arr[_i]); if (_e) { _s += _e; free(_e); } }
                _s += "}";
                free(arr);
                return strdup(_s.c_str());
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        lit.getContent(), lit.getContentSize(), arg0.getContent(), arg0.getContentSize(), arg1.getContent(), arg1.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterDatespansetBinsPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "DatespansetBinsPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return DatespansetBinsPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
