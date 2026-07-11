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

#include <Functions/Meos/TfloatCosPhysicalFunction.hpp>

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

TfloatCosPhysicalFunction::TfloatCosPhysicalFunction(PhysicalFunction trajFunction)
{
    parameterFunctions.reserve(1);
    parameterFunctions.push_back(std::move(trajFunction));
}

VarVal TfloatCosPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto traj = parameterValues[0].cast<VariableSizedData>();

    // Parse the hex-WKB operand, call f(Temporal*) -> Temporal*, and serialize
    // the temporal result back to hex-WKB inside the invoke; the heap string is
    // copied into the arena below. The operand and the MEOS result are freed here.
    auto hexStr = nautilus::invoke(
        +[](const char* aPtr, uint32_t aSize) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string aHex(aPtr, aSize);
                Temporal* a = temporal_from_hexwkb(aHex.c_str());
                if (!a) return (char*) nullptr;
                Temporal* res = tfloat_cos(a);
                free(a);
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
        traj.getContent(), traj.getContentSize());

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTfloatCosPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1,
                 "TfloatCosPhysicalFunction requires 1 child but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    return TfloatCosPhysicalFunction(std::move(arg0));
}

} // namespace NES
