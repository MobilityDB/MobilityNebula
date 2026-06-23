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

#include <Functions/Meos/MulTnumberTnumberPhysicalFunction.hpp>

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

MulTnumberTnumberPhysicalFunction::MulTnumberTnumberPhysicalFunction(PhysicalFunction value1Function,
                                                                     PhysicalFunction value2Function,
                                                                     PhysicalFunction tsFunction)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(value1Function));
    parameterFunctions.push_back(std::move(value2Function));
    parameterFunctions.push_back(std::move(tsFunction));
}

VarVal MulTnumberTnumberPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
        parameterValues.emplace_back(function.execute(record, arena));

    auto value1 = parameterValues[0].cast<nautilus::val<double>>();
    auto value2 = parameterValues[1].cast<nautilus::val<double>>();
    auto ts     = parameterValues[2].cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](double value1, double value2, uint64_t ts) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                const std::string tsStr = MEOS::Meos::convertEpochToTimestamp(ts);
                std::string wkt1 = fmt::format("{}@{}", value1, tsStr);
                std::string wkt2 = fmt::format("{}@{}", value2, tsStr);
                Temporal* t1 = tfloat_in(wkt1.c_str());
                Temporal* t2 = tfloat_in(wkt2.c_str());
                if (!t1 || !t2) { free(t1); free(t2); return 0.0; }
                Temporal* res = mul_tnumber_tnumber(t1, t2);
                free(t1); free(t2);
                if (!res) return 0.0;
                double r = tfloat_start_value(res);
                free(res);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        value1, value2, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterMulTnumberTnumberPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "MulTnumberTnumberPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return MulTnumberTnumberPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
