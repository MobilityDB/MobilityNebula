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

#include <Functions/Meos/EverNeTintIntPhysicalFunction.hpp>

#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
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
namespace NES::PhysicalFunctionGeneratedRegistrar { PhysicalFunctionRegistryReturnType RegisterEverNeTintIntPhysicalFunction(PhysicalFunctionRegistryArguments); }

namespace NES {

EverNeTintIntPhysicalFunction::EverNeTintIntPhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction timestampFunction,
                                                          PhysicalFunction scalarFunction)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(timestampFunction));
    parameterFunctions.push_back(std::move(scalarFunction));
}

VarVal EverNeTintIntPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value     = parameterValues[0].cast<nautilus::val<int>>();
    auto timestamp = parameterValues[1].cast<nautilus::val<uint64_t>>();
    auto scalar    = parameterValues[2].cast<nautilus::val<int>>();

    const auto result = nautilus::invoke(
        +[](int valueValue,
            uint64_t timestampValue,
            int scalarValue) -> int {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                const std::string tsString = MEOS::Meos::convertEpochToTimestamp(timestampValue);
                std::string wkt = fmt::format("{}@{}", valueValue, tsString);
                Temporal* temp = tint_in(wkt.c_str());
                if (!temp) return 0;
                int r = ever_ne_tint_int(temp, scalarValue);
                free(temp);
                return r;
            }
            catch (const std::exception&)
            {
                return 0;
            }
        },
        value, timestamp, scalar);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverNeTintIntPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "EverNeTintIntPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return EverNeTintIntPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
