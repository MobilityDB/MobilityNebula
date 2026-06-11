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

#include <Functions/Meos/OverleftTspatialStboxPhysicalFunction.hpp>

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

OverleftTspatialStboxPhysicalFunction::OverleftTspatialStboxPhysicalFunction(PhysicalFunction lonFunction,
                                                          PhysicalFunction latFunction,
                                                          PhysicalFunction tsFunction,
                                                          PhysicalFunction arg0Function)
{
    parameterFunctions.reserve(4);
    parameterFunctions.push_back(std::move(lonFunction));
    parameterFunctions.push_back(std::move(latFunction));
    parameterFunctions.push_back(std::move(tsFunction));
    parameterFunctions.push_back(std::move(arg0Function));
}

VarVal OverleftTspatialStboxPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    std::vector<VarVal> parameterValues;
    parameterValues.reserve(parameterFunctions.size());
    for (const auto& function : parameterFunctions)
    {
        parameterValues.emplace_back(function.execute(record, arena));
    }

    auto lon = parameterValues[0].cast<nautilus::val<double>>();
    auto lat = parameterValues[1].cast<nautilus::val<double>>();
    auto ts = parameterValues[2].cast<nautilus::val<uint64_t>>();
    auto arg0 = parameterValues[3].cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](double lon,
            double lat,
            uint64_t ts,
            const char* arg0Ptr, uint32_t arg0Size) -> bool {
            try
            {
                MEOS::Meos::ensureMeosInitialized();
                if (!(lon >= -180.0 && lon <= 180.0 && lat >= -90.0 && lat <= 90.0)) return false;
                std::string tempWkt = fmt::format("SRID=4326;Point({} {})@{}", lon, lat, MEOS::Meos::convertEpochToTimestamp(ts));
                Temporal* temp = tgeompoint_in(tempWkt.c_str());
                if (!temp) return false;
                std::string arg0S(arg0Ptr, arg0Size);
                while (!arg0S.empty() && (arg0S.front()=='\'' || arg0S.front()=='"')) arg0S.erase(arg0S.begin());
                while (!arg0S.empty() && (arg0S.back()=='\'' || arg0S.back()=='"')) arg0S.pop_back();
                STBox* arg0B = stbox_in(arg0S.c_str());
                if (!arg0B) { free(temp); return false; }

                bool r = overleft_tspatial_stbox(temp, arg0B);
                free(temp);
                free(arg0B);
                return r;
            }
            catch (const std::exception&)
            {
                return false;
            }
        },
        lon, lat, ts, arg0.getContent(), arg0.getContentSize());

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterOverleftTspatialStboxPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "OverleftTspatialStboxPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    auto arg0 = std::move(arguments.childFunctions[0]);
    auto arg1 = std::move(arguments.childFunctions[1]);
    auto arg2 = std::move(arguments.childFunctions[2]);
    auto arg3 = std::move(arguments.childFunctions[3]);
    return OverleftTspatialStboxPhysicalFunction(std::move(arg0), std::move(arg1), std::move(arg2), std::move(arg3));
}

} // namespace NES
