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

#include <Functions/Meos/GeoTransformPipelinePhysicalFunction.hpp>
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

GeoTransformPipelinePhysicalFunction::GeoTransformPipelinePhysicalFunction(PhysicalFunction wkt, PhysicalFunction pipeline, PhysicalFunction srid_to, PhysicalFunction is_forward)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(wkt));
    paramFns.push_back(std::move(pipeline));
    paramFns.push_back(std::move(srid_to));
    paramFns.push_back(std::move(is_forward));
}

VarVal GeoTransformPipelinePhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto wkt = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto pipeline = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    auto srid_to = paramFns[2].execute(record, arena).cast<nautilus::val<uint64_t>>();
    auto is_forward = paramFns[3].execute(record, arena).cast<nautilus::val<uint64_t>>();
    constexpr uint32_t MAX_LEN = 16384;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* w, uint32_t wsz, const char* p, uint32_t psz, uint64_t srid_to, uint64_t is_forward, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz), ps(p, psz);
                GSERIALIZED* gs = geom_in(s.c_str(), -1);
                if (!gs) return 0u;
                GSERIALIZED* result = geo_transform_pipeline(gs, const_cast<char*>(ps.c_str()), (int32_t)srid_to, (bool)is_forward);
                free(gs);
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
        wkt, pipeline, srid_to, is_forward, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterGeoTransformPipelinePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "GeoTransformPipelinePhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return GeoTransformPipelinePhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
