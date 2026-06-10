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

#include <Functions/Meos/TintinstMakePhysicalFunction.hpp>

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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterTintinstMakePhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

TintinstMakePhysicalFunction::TintinstMakePhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(2);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal TintinstMakePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<nautilus::val<int32_t>>();
    auto arg0 = parameterValues[1].cast<nautilus::val<int64_t>>();

    // Parse the operands, call the MEOS set-algebra function, and serialize the
    // resulting span/set/spanset to its canonical text — all inside one invoke.
    // The heap string is copied into the arena below; operands and the MEOS
    // result are freed here. A null result yields a zero-length VARSIZED.
    auto outStr = nautilus::invoke(
        +[](int32_t value,
            int64_t arg0) -> char*
        {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                int temp = (int) value;

                Temporal* res = (Temporal*) tintinst_make(temp, arg0);
                if (!res) return (char*) nullptr;
                char* outStr = tint_out(res);
                free(res);
                return outStr;
            }
            catch (const std::exception&)
            {
                return (char*) nullptr;
            }
        },
        value, arg0);

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

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTintinstMakePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "TintinstMakePhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    return TintinstMakePhysicalFunction(std::move(arg0), std::move(arg1));
}

} // namespace NES
