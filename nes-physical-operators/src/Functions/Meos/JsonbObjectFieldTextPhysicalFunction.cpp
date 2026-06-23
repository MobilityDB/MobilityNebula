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

#include <Functions/Meos/JsonbObjectFieldTextPhysicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <MEOSWrapper.hpp>
#include <Nautilus/DataTypes/VarVal.hpp>
#include <Nautilus/DataTypes/VariableSizedData.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <ErrorHandling.hpp>
#include <ExecutionContext.hpp>
#include <function.hpp>
#include <utility>
#include <val.hpp>

extern "C" {
#include <meos.h>
#include <meos_json.h>
}
#include <string>
#include <string.h>

namespace NES {

JsonbObjectFieldTextPhysicalFunction::JsonbObjectFieldTextPhysicalFunction(PhysicalFunction jb, PhysicalFunction key) {
    paramFns.reserve(2);
    paramFns.push_back(std::move(jb));
    paramFns.push_back(std::move(key));
}

VarVal JsonbObjectFieldTextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto jb  = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto key = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    constexpr uint32_t MAX_LEN = 4096;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));
    const auto actualLen = nautilus::invoke(
        +[](const char* wj, uint32_t wjsz, const char* wk, uint32_t wksz,
            char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string sj(wj,wjsz), sk(wk,wksz);
                Jsonb* jb  = jsonb_in(sj.c_str());
                if (!jb) return 0u;
                text* key  = cstring_to_text(sk.c_str());
                text* res  = jsonb_object_field_text(jb, key);
                free(jb); free(key);
                if (!res) return 0u;
                char* out  = text_to_cstring(res);
                free(res);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        jb, key, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));
    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterJsonbObjectFieldTextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "JsonbObjectFieldTextPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return JsonbObjectFieldTextPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
