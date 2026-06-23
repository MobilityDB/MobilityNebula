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

#include <Functions/Meos/TextInitcapPhysicalFunction.hpp>
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
}
#include <string>
#include <string.h>

namespace NES {

TextInitcapPhysicalFunction::TextInitcapPhysicalFunction(PhysicalFunction str)
{
    paramFns.reserve(1);
    paramFns.push_back(std::move(str));
}

VarVal TextInitcapPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto str = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    constexpr uint32_t MAX_LEN = 4096;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));
    const auto actualLen = nautilus::invoke(
        +[](const char* w, uint32_t wsz, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                text* t   = cstring_to_text(s.c_str());
                if (!t) return 0u;
                text* res = text_initcap(t);
                free(t);
                if (!res) return 0u;
                char* out = text_to_cstring(res);
                free(res);
                if (!out) return 0u;
                uint32_t len = static_cast<uint32_t>(strlen(out));
                if (len > bufMax) len = bufMax;
                memcpy(buf, out, len);
                free(out);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        str, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));
    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTextInitcapPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==1,
                 "TextInitcapPhysicalFunction requires 1 child but got {}",
                 arguments.childFunctions.size());
    return TextInitcapPhysicalFunction(std::move(arguments.childFunctions[0]));
}

} // namespace NES
