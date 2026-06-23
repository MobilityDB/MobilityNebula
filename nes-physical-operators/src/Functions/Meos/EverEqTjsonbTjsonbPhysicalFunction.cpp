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

#include <Functions/Meos/EverEqTjsonbTjsonbPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <utility>
#include <val.hpp>
#include <stdlib.h>
#include <string.h>

extern "C" {
#include <meos.h>
#include <meos_json.h>
}

namespace NES {

EverEqTjsonbTjsonbPhysicalFunction::EverEqTjsonbTjsonbPhysicalFunction(PhysicalFunction json1, PhysicalFunction ts1, PhysicalFunction json2, PhysicalFunction ts2)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(json1));
    paramFns.push_back(std::move(ts1));
    paramFns.push_back(std::move(json2));
    paramFns.push_back(std::move(ts2));
}

VarVal EverEqTjsonbTjsonbPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto json1 = paramFns[0].execute(record, arena);
    auto ts1 = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto json2 = paramFns[2].execute(record, arena);
    auto ts2 = paramFns[3].execute(record, arena).cast<uint64_t>();
    const auto result = nautilus::invoke(
        +[](const char* json1, uint32_t json1_len, uint64_t ts1, const char* json2, uint32_t json2_len, uint64_t ts2) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* s1 = (char*)malloc(json1_len + 1);
                memcpy(s1, json1, json1_len); s1[json1_len] = '\0';
                Jsonb* jb1 = jsonb_in(s1); free(s1);
                if (!jb1) return 0.0;
                TInstant* inst1 = tjsonbinst_make(jb1, (TimestampTz)ts1);
                free(jb1);
                if (!inst1) return 0.0;
                char* s2 = (char*)malloc(json2_len + 1);
                memcpy(s2, json2, json2_len); s2[json2_len] = '\0';
                Jsonb* jb2 = jsonb_in(s2); free(s2);
                if (!jb2) { free(inst1); return 0.0; }
                TInstant* inst2 = tjsonbinst_make(jb2, (TimestampTz)ts2);
                free(jb2);
                if (!inst2) { free(inst1); return 0.0; }
                int r = ever_eq_tjsonb_tjsonb((Temporal*)inst1, (Temporal*)inst2);
                free(inst1); free(inst2);
                return r > 0 ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        json1, ts1, json2, ts2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverEqTjsonbTjsonbPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "EverEqTjsonbTjsonbPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return EverEqTjsonbTjsonbPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]),
                                  std::move(arguments.childFunctions[3]));
}

} // namespace NES
