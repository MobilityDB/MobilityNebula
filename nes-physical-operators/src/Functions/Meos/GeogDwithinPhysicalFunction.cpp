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

#include <Functions/Meos/GeogDwithinPhysicalFunction.hpp>
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

GeogDwithinPhysicalFunction::GeogDwithinPhysicalFunction(PhysicalFunction wkt1, PhysicalFunction wkt2,
                                              PhysicalFunction tolerance)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(wkt1));
    paramFns.push_back(std::move(wkt2));
    paramFns.push_back(std::move(tolerance));
}

VarVal GeogDwithinPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto s1  = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto s2  = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    auto tol = paramFns[2].execute(record, arena).cast<double>();

    const auto result = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz,
            const char* w2, uint32_t w2sz,
            double tol) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1, w1sz);
                std::string s2(w2, w2sz);
                GSERIALIZED* gs1 = geom_in(s1.c_str(), -1);
                if (!gs1) return 0.0;
                GSERIALIZED* gs2 = geom_in(s2.c_str(), -1);
                if (!gs2) { free(gs1); return 0.0; }
                double r = geog_dwithin(gs1, gs2, tol, true) ? 1.0 : 0.0;
                free(gs1); free(gs2);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        s1, s2, tol);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeogDwithinPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "GeogDwithinPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return GeogDwithinPhysicalFunction(std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
