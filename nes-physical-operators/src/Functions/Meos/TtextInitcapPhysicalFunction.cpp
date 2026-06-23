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

#include <Functions/Meos/TtextInitcapPhysicalFunction.hpp>
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

TtextInitcapPhysicalFunction::TtextInitcapPhysicalFunction(PhysicalFunction valueFunction,
                                              PhysicalFunction tsFunction)
{
    paramFns.reserve(2);
    paramFns.push_back(std::move(valueFunction));
    paramFns.push_back(std::move(tsFunction));
}

VarVal TtextInitcapPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto value = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto ts    = paramFns[1].execute(record, arena).cast<nautilus::val<uint64_t>>();

    constexpr uint32_t MAX_LEN = 4096;
    auto outBuf = arena.allocateVariableSizedData(nautilus::val<uint32_t>(MAX_LEN));

    const auto actualLen = nautilus::invoke(
        +[](const char* v, uint32_t vsz, uint64_t t, char* buf, uint32_t bufMax) -> uint32_t {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string val_str(v, vsz);
                while (!val_str.empty() && (val_str.front() == '\'' || val_str.front() == '"'))
                    val_str = val_str.substr(1);
                while (!val_str.empty() && (val_str.back()  == '\'' || val_str.back()  == '"'))
                    val_str = val_str.substr(0, val_str.size() - 1);
                std::string ts_str = MEOS::Meos::convertEpochToTimestamp(t);
                std::string wkt = "'" + val_str + "'@" + ts_str;
                Temporal* temp = ttext_in(wkt.c_str());
                if (!temp) return 0u;
                Temporal* res = ttext_initcap(temp);
                free(temp);
                if (!res) return 0u;
                text* txt = ttext_start_value(res);
                free(res);
                if (!txt) return 0u;
                char* cstr = text_to_cstring(txt);
                free(txt);
                if (!cstr) return 0u;
                uint32_t len = static_cast<uint32_t>(std::strlen(cstr));
                if (len > bufMax) len = bufMax;
                std::memcpy(buf, cstr, len);
                free(cstr);
                return len;
            } catch (const std::exception&) { return 0u; }
        },
        value, ts, outBuf.getContent(), nautilus::val<uint32_t>(MAX_LEN));

    VarVal(actualLen).writeToMemory(outBuf.getReference());
    return outBuf;
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterTtextInitcapPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 2,
                 "TtextInitcapPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return TtextInitcapPhysicalFunction(
        std::move(arguments.childFunctions[0]),
        std::move(arguments.childFunctions[1]));
}

} // namespace NES
