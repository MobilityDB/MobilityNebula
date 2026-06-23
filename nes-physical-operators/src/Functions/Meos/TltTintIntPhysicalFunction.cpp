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

#include <Functions/Meos/TltTintIntPhysicalFunction.hpp>

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

namespace NES {

TltTintIntPhysicalFunction::TltTintIntPhysicalFunction(PhysicalFunction valueFunction,
                                      PhysicalFunction thresholdFunction,
                                      PhysicalFunction tsFunction)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(thresholdFunction));
    parameterFunctions.push_back(std::move(tsFunction));
}

VarVal TltTintIntPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
        parameterValues.emplace_back(function.execute(record, arena));

    auto value = parameterValues[0].cast<nautilus::val<double>>();
    auto thresh = parameterValues[1].cast<nautilus::val<double>>();
    auto ts     = parameterValues[2].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double v, double s, uint64_t t) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string ts_str = MEOS::Meos::convertEpochToTimestamp(t);
                std::string wkt = fmt::format("{}", static_cast<int>(v)) + "@" + ts_str;
                Temporal* temp = tint_in(wkt.c_str());
                if (!temp) return 0.0;
                int iv = static_cast<int>(s);
                Temporal* res = tlt_tint_int(temp, iv);
                free(temp);
                if (!res) return 0.0;
                bool r = tbool_start_value(res);
                free(res);
                return static_cast<double>(r ? 1.0 : 0.0);
            } catch (const std::exception&) { return 0.0; }
        },
        value, thresh, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTltTintIntPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "TltTintIntPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return TltTintIntPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
