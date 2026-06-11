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

#include <Functions/Meos/AfterTboxTnumberPhysicalFunction.hpp>

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

AfterTboxTnumberPhysicalFunction::AfterTboxTnumberPhysicalFunction(PhysicalFunction valueFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(3);
    parameterFunctions.push_back(std::move(valueFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal AfterTboxTnumberPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto value = parameterValues[0].cast<nautilus::val<double>>();
    auto ts = parameterValues[1].cast<nautilus::val<uint64_t>>();
    auto arg0 = parameterValues[2].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double value,
            uint64_t ts,
            const char* arg0Ptr, uint32_t arg0Size) -> bool {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                std::string tempWkt = fmt::format("{}@{}", value, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tfloat_in(tempWkt.c_str());
                if (!temp) return false;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                TBox* arg0B = tbox_in(arg0S.c_str());
                if (!arg0B) { free(temp); return false; }

                bool r = after_tbox_tnumber(arg0B, temp);
                free(temp);
                free(arg0B);
                return r;
            }
            catch (const std::exception&)
            {
                return false;
            }
        },
        value, ts, arg0.getContent(), arg0.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAfterTboxTnumberPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "AfterTboxTnumberPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    return AfterTboxTnumberPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2));
}

} // namespace NES
