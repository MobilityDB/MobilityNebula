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

#include <Functions/Meos/TfloatValueTimeBoxesPhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTfloatValueTimeBoxesPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TfloatValueTimeBoxesPhysicalFunction::TfloatValueTimeBoxesPhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function,
                                                          PhysicalFunction arg2Function,
                                                          PhysicalFunction arg3Function)
{
    parameterFunctions.reserve(6);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
    parameterFunctions.push_back(std::move(arg2Function));
    parameterFunctions.push_back(std::move(arg3Function));
}

VarVal TfloatValueTimeBoxesPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<nautilus::val<double>>();
    auto ts = parameterValues[1].cast<nautilus::val<uint64_t>>();
    auto arg0 = parameterValues[2].cast<nautilus::val<double>>();
    auto arg1 = parameterValues[3].cast<VariableSizedData>();
    auto arg2 = parameterValues[4].cast<nautilus::val<double>>();
    auto arg3 = parameterValues[5].cast<VariableSizedData>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](double value,
            uint64_t ts,
            double arg0,
            const char* arg1Ptr, uint32_t arg1Size,
            double arg2,
            const char* arg3Ptr, uint32_t arg3Size) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempWkt = fmt::format("{}@{}", value, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tfloat_in(tempWkt.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg1S(arg1Ptr, arg1Size);
                while (!arg1S.empty() && (arg1S.front()=='\'' || arg1S.front()=='"')) arg1S.erase(arg1S.begin());
                while (!arg1S.empty() && (arg1S.back()=='\'' || arg1S.back()=='"')) arg1S.pop_back();
                Interval* arg1B = interval_in(arg1S.c_str(), -1);
                if (!arg1B) { free(temp); return (char*) nullptr; }
                std::string arg3S(arg3Ptr, arg3Size);
                while (!arg3S.empty() && (arg3S.front()=='\'' || arg3S.front()=='"')) arg3S.erase(arg3S.begin());
                while (!arg3S.empty() && (arg3S.back()=='\'' || arg3S.back()=='"')) arg3S.pop_back();
                TimestampTz arg3V = timestamptz_in(arg3S.c_str(), -1);

                int _cnt = 0;
                TBox* arr = (TBox*) tfloat_value_time_boxes(temp, arg0, arg1B, arg2, arg3V, &_cnt);
                free(temp);
                free(arg1B);
                if (!arr || _cnt <= 0) return (char*) nullptr;
                std::string _s = "{";
                for (int _i = 0; _i < _cnt; _i++) { if (_i) _s += ", "; char* _e = tbox_out(&arr[_i], 15); if (_e) { _s += _e; free(_e); } }
                _s += "}";
                free(arr);
                return strdup(_s.c_str());
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        value, ts, arg0, arg1.getContent(), arg1.getContentSize(), arg2, arg3.getContent(), arg3.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTfloatValueTimeBoxesPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 6,
                 "TfloatValueTimeBoxesPhysicalFunction requires 6 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    auto arg4 = std::move(arguments.childFunctions[4]);
    auto arg5 = std::move(arguments.childFunctions[5]);
    return TfloatValueTimeBoxesPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3), std::move(arg4), std::move(arg5));
}

} // namespace NES
