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

/* The kernels of the functions of MobilityNebula itself, which the MEOS catalog does not state: the
 * version of MobilityNebula under its own name, beside the version of the MEOS it links, which the
 * catalog's kernels #Meos_meos_version_0 and #Meos_meos_full_version_0 answer. Each binding of MEOS
 * answers its own version so (MobilityDuck mobilityduckVersion, PyMEOS pymeos_version). The kernels
 * and their registration follow those of MeosKernels0.cpp. */

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <string>
#include <utility>

extern "C" {
#include <meos.h>
}

#include <any>
#include <DataTypes/DataType.hpp>
#include <Functions/MeosPhysicalFunction.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Plugins/PluginDescriptor.hpp>
#include <MeosKernelRuntime.hpp>
#include <PhysicalFunctionRegistry.hpp>

namespace NES
{

namespace
{
/// The version of MobilityNebula, the MEOS line its plugin implements, as the plugin's CMake states it
constexpr const char* mobilityNebulaVersion = "MobilityNebula " MOBILITYNEBULA_VERSION;

/// A copy of a string the caller frees, as #MeosRuntime::string frees the string MEOS answers
char* copied(const std::string& value)
{
    auto* copy = static_cast<char*>(std::malloc(value.size() + 1));
    std::memcpy(copy, value.c_str(), value.size() + 1);
    return copy;
}

int8_t* Mobilitynebula_version_0(int8_t* out)
{
    static_cast<void>(out);
    return MeosRuntime::string(copied(mobilityNebulaVersion));
}

int8_t* Mobilitynebula_full_version_0(int8_t* out)
{
    MeosRuntime::initialized();
    static_cast<void>(out);
    size_t length0 = 0;
    meos_errno_reset();
    auto* meos = meos_full_version(&length0);
    MeosRuntime::raised("meos_full_version");
    std::string full = std::string{mobilityNebulaVersion} + ", " + std::string{meos};
    std::free(meos);
    return MeosRuntime::string(copied(full));
}

void describe(PluginDescriptor& descriptor)
{
    descriptor.entries.push_back(
        EntryProvision{
            .registryClassName = "PhysicalFunctionRegistry",
            .key = "Mobilitynebula_version_0",
            .entry = std::any{PhysicalFunctionRegistry::EntryType{
                [](PhysicalFunctionRegistryArguments arguments) -> PhysicalFunction
                {
                    return MeosPhysicalFunction(
                        MeosKernel{&Mobilitynebula_version_0}, {}, DataType::Type::VARSIZED, false, std::move(arguments.childFunctions));
                }}}});
    descriptor.entries.push_back(
        EntryProvision{
            .registryClassName = "PhysicalFunctionRegistry",
            .key = "Mobilitynebula_full_version_0",
            .entry = std::any{PhysicalFunctionRegistry::EntryType{
                [](PhysicalFunctionRegistryArguments arguments) -> PhysicalFunction
                {
                    return MeosPhysicalFunction(
                        MeosKernel{&Mobilitynebula_full_version_0},
                        {},
                        DataType::Type::VARSIZED,
                        false,
                        std::move(arguments.childFunctions));
                }}}});
}

/// Static registration is the purpose of this initializer; it only stores a function pointer.
/// NOLINTNEXTLINE(bugprone-throwing-static-initialization)
const auto collected = []
{
    BuiltinPlugins::instance().addDescriber(&describe);
    return 0;
}();

}

}
