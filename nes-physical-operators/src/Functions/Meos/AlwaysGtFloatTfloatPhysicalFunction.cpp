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

#include <Functions/Meos/AlwaysGtFloatTfloatPhysicalFunction.hpp>

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

AlwaysGtFloatTfloatPhysicalFunction::AlwaysGtFloatTfloatPhysicalFunction(PhysicalFunction scalarFunction,
                                                                      PhysicalFunction valueFunction,
                                                                      PhysicalFunction tsFunction)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(scalarFunction));
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(tsFunction));
}

VarVal AlwaysGtFloatTfloatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
        parameterValues.emplace_back(function.execute(record, arena));

    auto scalar = parameterValues[0].cast<nautilus::val<double>>();
    auto value  = parameterValues[1].cast<nautilus::val<double>>();
    auto ts     = parameterValues[2].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double d, double v, uint64_t t) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string ts_str = MEOS::Meos::convertEpochToTimestamp(t);
                std::string wkt = fmt::format("{}@{}", v, ts_str);
                Temporal* temp = tfloat_in(wkt.c_str());
                if (!temp) return 0.0;
                int r = always_gt_float_tfloat(d, temp);
                free(temp);
                return static_cast<double>(r);
            } catch (const std::exception&) { return 0.0; }
        },
        scalar, value, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysGtFloatTfloatPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "AlwaysGtFloatTfloatPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return AlwaysGtFloatTfloatPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
