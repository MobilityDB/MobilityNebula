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

#include <Functions/Meos/ContainsFloatspanSpanPhysicalFunction.hpp>
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

ContainsFloatspanSpanPhysicalFunction::ContainsFloatspanSpanPhysicalFunction(PhysicalFunction sp1, PhysicalFunction sp2) {
    paramFns.reserve(2);
    paramFns.push_back(std::move(sp1));
    paramFns.push_back(std::move(sp2));
}

VarVal ContainsFloatspanSpanPhysicalFunction::execute(const Record& record, ArenaRef& arena) const {
    auto sp1 = paramFns[0].execute(record, arena).cast<VariableSizedData>();
    auto sp2 = paramFns[1].execute(record, arena).cast<VariableSizedData>();
    const auto result = nautilus::invoke(
        +[](const char* w1, uint32_t w1sz, const char* w2, uint32_t w2sz) -> double {
            try {
                MEOS::Meos::ensureMeosInitialized();
                std::string s1(w1,w1sz), s2(w2,w2sz);
                Span* sp1 = floatspan_in(s1.c_str());
                if (!sp1) return 0.0;
                Span* sp2 = floatspan_in(s2.c_str());
                if (!sp2) { free(sp1); return 0.0; }
                bool r = contains_span_span(sp1, sp2);
                free(sp1); free(sp2);
                return r ? 1.0 : 0.0;
            } catch (const std::exception&) { return 0.0; }
        },
        sp1, sp2);
    return VarVal(result);
}

PhysicalFunctionRegistryReturnType PhysicalFunctionGeneratedRegistrar::RegisterContainsFloatspanSpanPhysicalFunction(
    PhysicalFunctionRegistryArguments arguments)
{
    PRECONDITION(arguments.childFunctions.size()==2,
                 "ContainsFloatspanSpanPhysicalFunction requires 2 children but got {}",
                 arguments.childFunctions.size());
    return ContainsFloatspanSpanPhysicalFunction(
                                  std::move(arguments.childFunctions[0]),
                                  std::move(arguments.childFunctions[1]));
}

} // namespace NES
