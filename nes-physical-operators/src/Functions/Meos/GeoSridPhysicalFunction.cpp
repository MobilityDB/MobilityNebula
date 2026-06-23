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

#include <Functions/Meos/GeoSridPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <string>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_geo.h>
}

namespace NES {

GeoSridPhysicalFunction::GeoSridPhysicalFunction(PhysicalFunction wkt)
{
    paramFns.reserve(1);
    paramFns.push_back(std::move(wkt));
}

VarVal GeoSridPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto s1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1, w1sz);
                GSERIALIZED* gs1 = geom_in(s1.c_str(), -1);
                if (!gs1) return 0.0;
                double r = (double)geo_srid(gs1);
                free(gs1);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        s1);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeoSridPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 1,
                 "GeoSridPhysicalFunction requires 1 child but got {}",
                 arguments.childFunctions.size());
    return GeoSridPhysicalFunction(std::move(arguments.childFunctions[0]));
}

} // namespace NES
