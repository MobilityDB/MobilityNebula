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

#include <Functions/Meos/StboxHaszPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <fmt/format.h>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterStboxHaszPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

StboxHaszPhysicalFunction::StboxHaszPhysicalFunction(PhysicalFunction boxFunction)
{
    parameterFunctions.reserve(1);
    parameterFunctions.push_back(std::move(boxFunction));
}

VarVal StboxHaszPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto box = parameterValues[0].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](const char* boxPtr, uint32_t boxSize) -> bool {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempS(boxPtr, boxSize);
                while (!tempS.empty() && (tempS.front()=='\'' || tempS.front()=='"')) tempS.erase(tempS.begin());
                while (!tempS.empty() && (tempS.back()=='\'' || tempS.back()=='"')) tempS.pop_back();
                STBox* temp = stbox_in(tempS.c_str());
                if (!temp) return false;

                bool r = stbox_hasz(temp);
                free(temp);
                return r;
            }
            catch (const std::exception&)
            {
                return false;
            }
        },
        box.getContent(), box.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterStboxHaszPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1,
                 "StboxHaszPhysicalFunction requires 1 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    return StboxHaszPhysicalFunction(std::move(arg0));
}

} // namespace NES
