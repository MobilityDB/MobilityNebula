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

#include <Functions/Meos/TrgeometryAtStboxPhysicalFunction.hpp>

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
#include <meos_rgeo.h>
}

namespace NES {

TrgeometryAtStboxPhysicalFunction::TrgeometryAtStboxPhysicalFunction(PhysicalFunction trajFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(trajFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
}

VarVal TrgeometryAtStboxPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto traj = parameterValues[0].cast<VariableSizedData>();
    auto arg0 = parameterValues[1].cast<VariableSizedData>();
    auto arg1 = parameterValues[2].cast<nautilus::val<bool>>();

    // Call MEOS f over one hex-WKB temporal operand, a static STBox (WKT/text literal),
    // and a border-inclusion flag, and serialize the temporal result back to hex-WKB
    // inside the invoke; the returned heap string is copied into the arena below. The
    // operand, the parsed STBox, and the MEOS result are freed here.
    auto hexStr = nautilus::invoke(
        +[](const char* aPtr, uint32_t aSize, const char* bPtr, uint32_t bSize, bool borderInc) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string aHex(aPtr, aSize);
                Temporal* a = temporal_from_hexwkb(aHex.c_str());
                if (!a) return (char*) nullptr;
                std::string bS(bPtr, bSize);
                while (!bS.empty() && (bS.front()=='\'' || bS.front()=='"')) bS.erase(bS.begin());
                while (!bS.empty() && (bS.back()=='\'' || bS.back()=='"')) bS.pop_back();
                STBox* box = stbox_in(bS.c_str());
                if (!box) { free(a); return (char*) nullptr; }
                Temporal* res = trgeometry_at_stbox(a, box, borderInc);
                free(a);
                free(box);
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
        traj.getContent(), traj.getContentSize(), arg0.getContent(), arg0.getContentSize(), arg1);

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTrgeometryAtStboxPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "TrgeometryAtStboxPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return TrgeometryAtStboxPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
