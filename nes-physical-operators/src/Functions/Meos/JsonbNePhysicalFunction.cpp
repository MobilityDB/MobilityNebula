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

#include <Functions/Meos/JsonbNePhysicalFunction.hpp>
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

JsonbNePhysicalFunction::JsonbNePhysicalFunction(PhysicalFunction jb1, PhysicalFunction jb2) {
    paramFns.reserve(2);
    paramFns.push_back(std::move(jb1));
    paramFns.push_back(std::move(jb2));
}

VarVal JsonbNePhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto jb1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto jb2 = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    const auto result = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz, const char* w2, uint32_t w2sz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1,w1sz), s2(w2,w2sz);
                Jsonb* jb1 = jsonb_in(s1.c_str());
                if (!jb1) return 0.0;
                Jsonb* jb2 = jsonb_in(s2.c_str());
                if (!jb2) { free(jb1); return 0.0; }
                bool r = jsonb_ne(jb1, jb2);
                free(jb1); free(jb2);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        jb1, jb2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterJsonbNePhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "JsonbNePhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return JsonbNePhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
