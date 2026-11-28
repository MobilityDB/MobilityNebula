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

/* The functions of MobilityNebula itself by their SQL names, which the MEOS catalog does not state: the
 * version of MobilityNebula, beside meosVersion and meosFullVersion of #meosFunctions, each a function
 * of no argument answering text, as those of MEOS are. Their kernels are in MobilityNebulaKernels.cpp. */

#include <array>
#include <span>
#include <string_view>
#include <Functions/MeosLogicalFunction.hpp>

namespace NES
{

namespace
{
constexpr std::array<std::string_view, 0> noArguments{};
constexpr std::array<MeosOverload, 1> fullVersion{MeosOverload{noArguments, "VARSIZED", "Mobilitynebula_full_version_0", false}};
constexpr std::array<MeosOverload, 1> version{MeosOverload{noArguments, "VARSIZED", "Mobilitynebula_version_0", false}};
}

std::span<const MeosFunction> mobilityNebulaFunctions()
{
    static constexpr std::array functions{
        MeosFunction{"mobilitynebulaFullVersion", fullVersion},
        MeosFunction{"mobilitynebulaVersion", version},
    };
    return functions;
}

}
