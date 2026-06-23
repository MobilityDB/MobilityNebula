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

#include <Functions/Meos/TextcatTtextTtextPhysicalFunction.hpp>
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

TextcatTtextTtextPhysicalFunction::TextcatTtextTtextPhysicalFunction(PhysicalFunction value1Function,
                                              PhysicalFunction ts1Function,
                                              PhysicalFunction value2Function,
                                              PhysicalFunction ts2Function)
{
    paramFns.reserve(4);
    paramFns.push_back(std::move(value1Function));
    paramFns.push_back(std::move(ts1Function));
    paramFns.push_back(std::move(value2Function));
    paramFns.push_back(std::move(ts2Function));
}

VarVal TextcatTtextTtextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto ts1    = paramFns[1].execute(record, arena).cast<nautilus::val<uint64_t>>();
    auto value2 = paramFns[2].execute(record, arena).cast<VariableSizedData>();
    auto ts2    = paramFns[3].execute(record, arena).cast<nautilus::val<uint64_t>>();

    constexpr uint32_t MAX_LEN = 8192;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* v1, uint32_t v1sz, uint64_t t1,
            const char* v2, uint32_t v2sz, uint64_t t2,
            char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                auto strip = [](std::string s) {
                    while (!s.empty() && (s.front() == '\'' || s.front() == '"')) s = s.substr(1);
                    while (!s.empty() && (s.back()  == '\'' || s.back()  == '"')) s = s.substr(0, s.size() - 1);
                    return s;
                };
                std::string s1 = strip(std::string(v1, v1sz));
                std::string s2 = strip(std::string(v2, v2sz));
                std::string ts1_str = MEOS::Meos::convertEpochToTimestamp(t1);
                std::string ts2_str = MEOS::Meos::convertEpochToTimestamp(t2);
                Temporal* temp1 = ttext_in(("'" + s1 + "'@" + ts1_str).c_str());
                if (!temp1) return 0u;
                Temporal* temp2 = ttext_in(("'" + s2 + "'@" + ts2_str).c_str());
                if (!temp2) { free(temp1); return 0u; }
                Temporal* res = textcat_ttext_ttext(temp1, temp2);
                free(temp1); free(temp2);
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
        value1, ts1, value2, ts2, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTextcatTtextTtextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 4,
                 "TextcatTtextTtextPhysicalFunction requires 4 children but got {}",
                 arguments.childFunctions.size());
    return TextcatTtextTtextPhysicalFunction(
        std::move(arguments.childFunctions[0]),
        std::move(arguments.childFunctions[1]),
        std::move(arguments.childFunctions[2]),
        std::move(arguments.childFunctions[3]));
}

} // namespace NES
