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

#include <Functions/Meos/AlwaysEqTjsonbJsonbPhysicalFunction.hpp>
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

AlwaysEqTjsonbJsonbPhysicalFunction::AlwaysEqTjsonbJsonbPhysicalFunction(PhysicalFunction json_str, PhysicalFunction ts, PhysicalFunction target_json)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(json_str));
    paramFns.push_back(std::move(ts));
    paramFns.push_back(std::move(target_json));
}

VarVal AlwaysEqTjsonbJsonbPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto json_str = paramFns[0].execute(record, arena);
    auto ts = paramFns[1].execute(record, arena).cast<uint64_t>();
    auto target_json = paramFns[2].execute(record, arena);
    const auto result = nautilus::invoke(
        +[](const char* json_str, uint32_t json_len, uint64_t ts, const char* target_json, uint32_t target_len) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                char* s1 = (char*)malloc(json_len + 1);
                memcpy(s1, json_str, json_len); s1[json_len] = '\0';
                Jsonb* jb = jsonb_in(s1); free(s1);
                if (!jb) return 0.0;
                TInstant* inst = tjsonbinst_make(jb, (TimestampTz)ts);
                free(jb);
                if (!inst) return 0.0;
                char* s2 = (char*)malloc(target_len + 1);
                memcpy(s2, target_json, target_len); s2[target_len] = '\0';
                Jsonb* target = jsonb_in(s2); free(s2);
                if (!target) { free(inst); return 0.0; }
                bool r = always_eq_tjsonb_jsonb((Temporal*)inst, target) > 0;
                free(inst); free(target);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        json_str, ts, target_json);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterAlwaysEqTjsonbJsonbPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "AlwaysEqTjsonbJsonbPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return AlwaysEqTjsonbJsonbPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]),
                                  std::move(arguments.childFunctions[2]));
}

} // namespace NES
