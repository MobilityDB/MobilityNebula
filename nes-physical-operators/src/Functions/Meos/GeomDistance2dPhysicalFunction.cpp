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

#include <Functions/Meos/GeomDistance2dPhysicalFunction.hpp>
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

GeomDistance2dPhysicalFunction::GeomDistance2dPhysicalFunction(PhysicalFunction wkt1Function,
                                              PhysicalFunction wkt2Function)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(wkt1Function));
    paramFns.push_back(std::move(wkt2Function));
}

VarVal GeomDistance2dPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto wkt2 = paramFns[1].execute(record, arena).cast<VariableSizedData>();

    const auto result = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz, const char* w2, uint32_t w2sz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1, w1sz), s2(w2, w2sz);
                GSERIALIZED* gs1 = geom_in(s1.c_str(), -1);
                if (!gs1) return 0.0;
                GSERIALIZED* gs2 = geom_in(s2.c_str(), -1);
                if (!gs2) { free(gs1); return 0.0; }
                double r = geom_distance2d(gs1, gs2);
                free(gs1); free(gs2);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        wkt1, wkt2);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeomDistance2dPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "GeomDistance2dPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return GeomDistance2dPhysicalFunction(
        std::move(arguments.childFunctions[0]),
        std::move(arguments.childFunctions[1]));
}

} // namespace NES
