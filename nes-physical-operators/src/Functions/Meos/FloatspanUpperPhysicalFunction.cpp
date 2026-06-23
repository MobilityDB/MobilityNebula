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

#include <Functions/Meos/FloatspanUpperPhysicalFunction.hpp>
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

FloatspanUpperPhysicalFunction::FloatspanUpperPhysicalFunction(PhysicalFunction sp) {
    paramFns.reserve(1);
    paramFns.push_back(std::move(sp));
}

VarVal FloatspanUpperPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto sp = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    const auto result = nautilus::invoke(
        +[](const char* w, uint32_t wsz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                Span* sp = floatspan_in(s.c_str());
                if (!sp) return 0.0;
                double r = (double)floatspan_upper(sp);
                free(sp);
                return r;
            } catch (const std::exception&) { return 0.0; }
        },
        sp);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterFloatspanUpperPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==1,
                 "FloatspanUpperPhysicalFunction requires 1 children but got {}",
                 arguments.childFunctions.size());
    return FloatspanUpperPhysicalFunction(
                                  std::move(arguments.childFunctions[0]));
}

} // namespace NES
