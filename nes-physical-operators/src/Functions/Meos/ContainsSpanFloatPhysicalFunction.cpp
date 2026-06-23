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

#include <Functions/Meos/ContainsSpanFloatPhysicalFunction.hpp>
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

namespace NES {

ContainsSpanFloatPhysicalFunction::ContainsSpanFloatPhysicalFunction(PhysicalFunction sp, PhysicalFunction val) {
    paramFns.reserve(2);
    paramFns.push_back(std::move(sp));
    paramFns.push_back(std::move(val));
}

VarVal ContainsSpanFloatPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto sp  = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto val = paramFns[1].execute(record, arena).cast<double>();
    const auto result = nautilus::invoke(
        +[](const char* w, uint32_t wsz, double val_d) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s(w, wsz);
                Span* sp = floatspan_in(s.c_str());
                if (!sp) return 0.0;
                bool r = contains_span_float(sp, val_d);
                free(sp);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        sp, val);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterContainsSpanFloatPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "ContainsSpanFloatPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return ContainsSpanFloatPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
