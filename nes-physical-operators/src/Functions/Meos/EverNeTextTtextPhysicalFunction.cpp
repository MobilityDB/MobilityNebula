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

#include <Functions/Meos/EverNeTextTtextPhysicalFunction.hpp>
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

EverNeTextTtextPhysicalFunction::EverNeTextTtextPhysicalFunction(
    PhysicalFunction valueFunction, PhysicalFunction refFunction, PhysicalFunction tsFunction)
{
    paramFns.reserve(3);
    paramFns.push_back(std::move(valueFunction));
    paramFns.push_back(std::move(refFunction));
    paramFns.push_back(std::move(tsFunction));
}

VarVal EverNeTextTtextPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    auto ref   = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto value = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    auto ts    = paramFns[2].execute(record, arena).cast<nautilus::val<uint64_t>>();

    const auto result = nautilus::invoke(
        +[](const char* r, uint32_t rsz, const char* v, uint32_t vsz, uint64_t t) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string ref_str(r, rsz);
                std::string val_str(v, vsz);
                while (!ref_str.empty() && (ref_str.front() == '\'' || ref_str.front() == '"')) ref_str = ref_str.substr(1);
                while (!ref_str.empty() && (ref_str.back()  == '\'' || ref_str.back()  == '"')) ref_str = ref_str.substr(0, ref_str.size()-1);
                while (!val_str.empty() && (val_str.front() == '\'' || val_str.front() == '"')) val_str = val_str.substr(1);
                while (!val_str.empty() && (val_str.back()  == '\'' || val_str.back()  == '"')) val_str = val_str.substr(0, val_str.size()-1);
                std::string ts_str = MEOS::Meos::convertEpochToTimestamp(t);
                std::string wkt = "'" + val_str + "'@" + ts_str;
                Temporal* temp = ttext_in(wkt.c_str());
                if (!temp) return 0.0;
                text* txt = cstring_to_text(ref_str.c_str());
                if (!txt) { free(temp); return 0.0; }
                int result = ever_ne_text_ttext(txt, temp);
                free(temp);
                free(txt);
                return static_cast<double>(result);
            } catch (const std::exception&) { return 0.0; }
        },
        ref, value, ts);

    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterEverNeTextTtextPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size() == 3,
                 "EverNeTextTtextPhysicalFunction requires 3 children but got {}",
                 arguments.childFunctions.size());
    return EverNeTextTtextPhysicalFunction(
        std::move(arguments.childFunctions[0]),
        std::move(arguments.childFunctions[1]),
        std::move(arguments.childFunctions[2]));
}

} // namespace NES
