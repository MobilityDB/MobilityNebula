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

#include <Functions/Meos/JsonbArrayLengthPhysicalFunction.hpp>
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

JsonbArrayLengthPhysicalFunction::JsonbArrayLengthPhysicalFunction(PhysicalFunction jb) {
    paramFns.reserve(1);
    paramFns.push_back(std::move(jb));
}

VarVal JsonbArrayLengthPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto jb = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    const auto result = nautilus::invoke(
        +[](const char* w, uint32_t wsz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                Jsonb* jb = jsonb_in(s.c_str());
                if (!jb) return 0.0;
                int r = jsonb_array_length(jb);
                free(jb);
                return (double)r;
            } catch (const std::exception&) { return 0.0; }
        },
        jb);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterJsonbArrayLengthPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==1,
                 "JsonbArrayLengthPhysicalFunction requires 1 children but got {}",
                 arguments.childFunctions.size());
    return JsonbArrayLengthPhysicalFunction(
                                  std::move(arguments.childFunctions[0]));
}

} // namespace NES
