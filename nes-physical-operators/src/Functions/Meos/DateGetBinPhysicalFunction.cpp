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

#include <Functions/Meos/DateGetBinPhysicalFunction.hpp>

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
}

/* Decoupled from the regenerated plugin registrar (see PhysicalFunctionRegistry.hpp): only the registry types are pulled in, and this operator declares its own Register function. */
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterDateGetBinPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

DateGetBinPhysicalFunction::DateGetBinPhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction arg0Function,
                                                          PhysicalFunction arg1Function)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(arg0Function));
    parameterFunctions.push_back(std::move(arg1Function));
}

VarVal DateGetBinPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<nautilus::val<int32_t>>();
    auto arg0 = parameterValues[1].cast<VariableSizedData>();
    auto arg1 = parameterValues[2].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](int32_t value,
            const char* arg0Ptr, uint32_t arg0Size,
            const char* arg1Ptr, uint32_t arg1Size) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                DateADT temp = (DateADT) value;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                Interval* arg0B = interval_in(arg0S.c_str(), -1);
                if (!arg0B) { return 0; }
                std::string arg1S(arg1Ptr, arg1Size);
                while (!arg1S.empty() && (arg1S.front()=='\'' || arg1S.front()=='"')) arg1S.erase(arg1S.begin());
                while (!arg1S.empty() && (arg1S.back()=='\'' || arg1S.back()=='"')) arg1S.pop_back();
                DateADT arg1V = date_in(arg1S.c_str());

                int r = date_get_bin(temp, arg0B, arg1V);
                free(arg0B);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        value, arg0.getContent(), arg0.getContentSize(), arg1.getContent(), arg1.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterDateGetBinPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "DateGetBinPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return DateGetBinPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
