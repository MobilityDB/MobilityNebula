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

#include <Functions/Meos/DivTnumberTnumberPhysicalFunction.hpp>

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

namespace NES {

DivTnumberTnumberPhysicalFunction::DivTnumberTnumberPhysicalFunction(PhysicalFunction trajFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(2);
    parameterFunctions.push_back(std::move(trajFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal DivTnumberTnumberPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto traj = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<VariableSizedData>();

    // Call MEOS f(Temporal*, Temporal*) -> Temporal* over the two hex-WKB
    // operands and serialize the result back to hex-WKB inside the invoke; the
    // returned heap string is copied into the arena below. Both operands and the
    // MEOS result are freed here.
    auto hexStr = nautilus::invoke(
        +[](const char* aPtr, uint32_t aSize, const char* bPtr, uint32_t bSize) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string aHex(aPtr, aSize);
                std::string bHex(bPtr, bSize);
                Temporal* a = temporal_from_hexwkb(aHex.c_str());
                if (!a) return (char*) nullptr;
                Temporal* b = temporal_from_hexwkb(bHex.c_str());
                if (!b) { free(a); return (char*) nullptr; }
                Temporal* res = div_tnumber_tnumber(a, b);
                free(a);
                free(b);
                if (!res) return (char*) nullptr;
                size_t hexSize = 0;
                char* hexOut = temporal_as_hexwkb(res, 0, &hexSize);
                free(res);
                return hexOut;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        traj.getContent(), traj.getContentSize(), arg0.getContent(), arg0.getContentSize());

    const auto hexLen = nautilus::invoke(
        +[](const char* s) -> uint32_t { return s ? (uint32_t) strlen(s) : (uint32_t) 0; },
        hexStr);

    auto variableSized = arena.allocateVariableSizedData(hexLen);

    nautilus::invoke(
        +[](int8_t* dest, const char* s, uint32_t len) -> void
        {
            if (s)
            {
                memcpy(dest, s, len);
                free((void*) s);
            }
        },
        variableSized.getContent(), hexStr, hexLen);

    return variableSized;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterDivTnumberTnumberPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "DivTnumberTnumberPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    return DivTnumberTnumberPhysicalFunction(std::move(arg0), std::move(arg1));
}

} // namespace NES
