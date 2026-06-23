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

#include <Functions/Meos/GeomShortestline2dPhysicalFunction.hpp>
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

GeomShortestline2dPhysicalFunction::GeomShortestline2dPhysicalFunction(PhysicalFunction wkt1, PhysicalFunction wkt2)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(wkt1));
    paramFns.push_back(std::move(wkt2));
}

VarVal GeomShortestline2dPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto wkt2 = paramFns[1].execute(record, arena).cast<VariableSizedData>();

    constexpr uint32_t MAX_LEN = 8192;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz, const char* w2, uint32_t w2sz, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1, w1sz), s2(w2, w2sz);
                GSERIALIZED* gs1 = geom_in(s1.c_str(), -1);
                if (!gs1) return 0u;
                GSERIALIZED* gs2 = geom_in(s2.c_str(), -1);
                if (!gs2) { free(gs1); return 0u; }
                GSERIALIZED* result = geom_shortestline2d(gs1, gs2);
                free(gs1); free(gs2);
                if (!result) return 0u;
                char* out = geo_as_text(result, -1);
                free(result);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        wkt1, wkt2, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeomShortestline2dPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "GeomShortestline2dPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return GeomShortestline2dPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
