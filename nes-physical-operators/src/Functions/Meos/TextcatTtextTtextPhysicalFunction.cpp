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

#include <Functions/Meos/TextcatTtextTtextPhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTextcatTtextTtextPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TextcatTtextTtextPhysicalFunction::TextcatTtextTtextPhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction value2Function,
                                                          PhysicalFunction ts2Function)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(value2Function));
    parameterFunctions.push_back(std::move(ts2Function));
}

VarVal TextcatTtextTtextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<VariableSizedData>();
    auto ts = parameterValues[1].cast<nautilus::val<uint64_t>>();
    auto value2 = parameterValues[2].cast<VariableSizedData>();
    auto ts2 = parameterValues[3].cast<nautilus::val<uint64_t>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](const char* valuePtr, uint32_t valueSize,
            uint64_t ts,
            const char* value2Ptr, uint32_t value2Size,
            uint64_t ts2) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempS(valuePtr, valueSize);
                while (!tempS.empty() && (tempS.front()=='\'' || tempS.front()=='"')) tempS.erase(tempS.begin());
                while (!tempS.empty() && (tempS.back()=='\'' || tempS.back()=='"')) tempS.pop_back();
                std::string tempWkt = fmt::format("\"{}\"@{}", tempS, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = ttext_in(tempWkt.c_str());
                if (!temp) return (char*) nullptr;
                std::string arg0tS(value2Ptr, value2Size);
                while (!arg0tS.empty() && (arg0tS.front()=='\'' || arg0tS.front()=='"')) arg0tS.erase(arg0tS.begin());
                while (!arg0tS.empty() && (arg0tS.back()=='\'' || arg0tS.back()=='"')) arg0tS.pop_back();
                std::string arg0tW = fmt::format("\"{}\"@{}", arg0tS, MEOS::Meos::convertEpochToTimestamp(ts2));
                Temporal* arg0t = ttext_in(arg0tW.c_str());
                if (!arg0t) { free(temp); return (char*) nullptr; }

                Temporal* res = textcat_ttext_ttext(temp, arg0t);
                free(temp);
                free(arg0t);
                if (!res) return (char*) nullptr;
                char* outStr = ttext_out(res);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        value.getContent(), value.getContentSize(), ts, value2.getContent(), value2.getContentSize(), ts2);

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTextcatTtextTtextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "TextcatTtextTtextPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    return TextcatTtextTtextPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
