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

#include <Functions/Meos/TextcatTtextTextPhysicalFunction.hpp>
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
}

namespace NES {

TextcatTtextTextPhysicalFunction::TextcatTtextTextPhysicalFunction(PhysicalFunction valueFunction,
                                              PhysicalFunction tsFunction,
                                              PhysicalFunction refFunction)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(valueFunction));
    paramFns.push_back(std::move(tsFunction));
    paramFns.push_back(std::move(refFunction));
}

VarVal TextcatTtextTextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto ts    = paramFns[1].execute(record, arena).cast<nautilus::val<uint64_t>>();
    auto ref   = paramFns[2].execute(record, arena).cast<VariableSizedData>();

    constexpr uint32_t MAX_LEN = 8192;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* v, uint32_t vsz, uint64_t t, const char* r, uint32_t rsz, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string val_str(v, vsz);
                while (!val_str.empty() && (val_str.front() == '\'' || val_str.front() == '"')) val_str = val_str.substr(1);
                while (!val_str.empty() && (val_str.back()  == '\'' || val_str.back()  == '"')) val_str = val_str.substr(0, val_str.size() - 1);
                std::string ref_str(r, rsz);
                while (!ref_str.empty() && (ref_str.front() == '\'' || ref_str.front() == '"')) ref_str = ref_str.substr(1);
                while (!ref_str.empty() && (ref_str.back()  == '\'' || ref_str.back()  == '"')) ref_str = ref_str.substr(0, ref_str.size() - 1);
                std::string ts_str = MEOS::Meos::convertEpochToTimestamp(t);
                std::string wkt = "'" + val_str + "'@" + ts_str;
                Temporal* temp = ttext_in(wkt.c_str());
                if (!temp) return 0u;
                text* txt = cstring_to_text(ref_str.c_str());
                if (!txt) { free(temp); return 0u; }
                Temporal* res = textcat_ttext_text(temp, txt);
                free(temp); free(txt);
                if (!res) return 0u;
                text* out_txt = ttext_start_value(res);
                free(res);
                if (!out_txt) return 0u;
                char* cstr = text_to_cstring(out_txt);
                free(out_txt);
                if (!cstr) return 0u;
                uint32_t len = static_cast<uint32_t>(std::strlen(cstr));
                if (len > bufMax) len = bufMax;
                std::memcpy(buf, cstr, len);
                free(cstr);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        value, ts, ref, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTextcatTtextTextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "TextcatTtextTextPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return TextcatTtextTextPhysicalFunction(
        std::move(arguments.childFunctions[0]),
        std::move(arguments.childFunctions[1]),
        std::move(arguments.childFunctions[2]));
}

} // namespace NES
