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

#pragma once
#include <string>
#include <vector>
#include <Functions/PhysicalFunction.hpp>
#include <Util/Registry.hpp>

namespace NES
{

using PhysicalFunctionRegistryReturnType = PhysicalFunction;

struct PhysicalFunctionRegistryArguments
{
    std::vector<PhysicalFunction> childFunctions;
};

class PhysicalFunctionRegistry
    : public BaseRegistry<PhysicalFunctionRegistry, std::string, PhysicalFunctionRegistryReturnType, PhysicalFunctionRegistryArguments>
{
};
}

/* MEOS: a plugin operator .cpp (which #defines NES_PLUGIN_OPERATOR_TU before
   including this header) needs only the registry types declared above to define
   its own Register<Op> function — not the generated registrar that lists every
   plugin. That .inc is regenerated whenever any operator is added, so including
   it here forces all generated operator TUs to recompile on every add. Skipping
   it for operator TUs decouples them; the central registerAll consumers (which do
   not define the macro) still get it. */
#ifndef NES_PLUGIN_OPERATOR_TU
#define INCLUDED_FROM_REGISTRY_PHYSICAL_FUNCTION
#include <PhysicalFunctionGeneratedRegistrar.inc>
#undef INCLUDED_FROM_REGISTRY_PHYSICAL_FUNCTION
#endif
