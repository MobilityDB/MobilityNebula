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

#include <Functions/Meos/JsonArrayElementTextPhysicalFunction.hpp>
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

JsonArrayElementTextPhysicalFunction::JsonArrayElementTextPhysicalFunction(PhysicalFunction js, PhysicalFunction idx)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(js));
    paramFns.push_back(std::move(idx));
}

VarVal JsonArrayElementTextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto js = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto idx = paramFns[1].execute(record, arena).cast<double>();
    constexpr uint32_t MAX_LEN = 4096;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* w, uint32_t wsz, double idx_d,
            char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                text* js  = json_in(s.c_str());
                if (!js) return 0u;
                text* res = json_array_element_text(js, (int)idx_d);
                free(js);
                if (!res) return 0u;
                char* out = json_out(res);
                free(res);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        js, idx, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterJsonArrayElementTextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "JsonArrayElementTextPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return JsonArrayElementTextPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
