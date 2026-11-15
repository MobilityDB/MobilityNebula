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

/// The readers of the MEOS types' text registered with the physical operators: the ValueDeserializer of each
/// type under its name, and under its name after `Nullable` for a field that may be null, the two names
/// provideValueDeserializer asks for. The entries are described to the plugin loader as the glue of an
/// add_registry_entry describes its one entry (cmake/RuntimeRegistrationUtil.cmake).

#include <algorithm>
#include <any>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <span>
#include <string>
#include <string_view>
#include <vector>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <ValueDeserializer.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Plugins/PluginDescriptor.hpp>
#include <Arena.hpp>
#include <MeosRegistration.hpp>
#include <ValueDeserializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

/// The bytes of the value the text states, in a buffer malloc allocates holding their length then the bytes,
/// as a MEOS kernel answers a variable-sized value
int8_t* readText(const MeosType* type, const int8_t* content, const uint64_t size)
{
    std::string terminated(size, '\0');
    std::memcpy(terminated.data(), content, size);
    const auto bytes = type->type->fromText(terminated);
    auto* answer = static_cast<int8_t*>(std::malloc(sizeof(uint32_t) + bytes.size()));
    const auto length = static_cast<uint32_t>(bytes.size());
    std::memcpy(answer, &length, sizeof(length));
    std::memcpy(answer + sizeof(uint32_t), bytes.data(), bytes.size());
    return answer;
}

/// The length of the bytes readText answers
uint64_t readLength(const int8_t* answer)
{
    uint32_t length = 0;
    std::memcpy(&length, answer, sizeof(length));
    return length;
}

/// The bytes readText answers copied where the record keeps them, and the answer freed
void keepRead(int8_t* destination, int8_t* answer, const uint64_t length)
{
    std::memcpy(destination, answer + sizeof(uint32_t), length);
    std::free(answer);
}

/// Whether the text is one the source states a null as
bool isNullText(const int8_t* content, const uint64_t size, const std::vector<std::string>* nullValues)
{
    std::string text(size, '\0');
    std::memcpy(text.data(), content, size);
    return std::ranges::find(*nullValues, text) != nullValues->end();
}

/// The ValueDeserializer of a MEOS type, reading the value its text states
class MeosValueDeserializer final : public ValueDeserializer
{
public:
    MeosValueDeserializer(const MeosType& type, const bool nullable, const bool quoted)
        : type(&type), nullable(nullable), quoted(quoted)
    {
    }

    [[nodiscard]] VarVal deserializeToVarVal(
        const nautilus::val<int8_t*>& fieldAddress,
        const nautilus::val<uint64_t>& fieldSize,
        const std::vector<std::string>& nullValues,
        const ArenaRef& arena) const override
    {
        nautilus::val<int8_t*> address = fieldAddress;
        nautilus::val<uint64_t> size = fieldSize;
        if (quoted)
        {
            address = address + nautilus::val<uint64_t>(1);
            size = size - nautilus::val<uint64_t>(2);
        }
        if (nullable)
        {
            if (nautilus::invoke(isNullText, address, size, nautilus::val<const std::vector<std::string>*>{&nullValues}))
            {
                return VarVal{arena.allocateVariableSizedData(nautilus::val<uint64_t>(0)), true, nautilus::val<bool>(true)};
            }
        }
        const auto answer = nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::Ref, .noUnwind = false},
            readText,
            nautilus::val<const MeosType*>{type},
            address,
            size);
        const auto length = nautilus::invoke(readLength, answer);
        auto value = arena.allocateVariableSizedData(length);
        nautilus::invoke(keepRead, value.getContent(), answer, length);
        return VarVal{value, nullable, nautilus::val<bool>(false)};
    }

private:
    const MeosType* type;
    bool nullable;
    bool quoted;
};

void describe(PluginDescriptor& descriptor)
{
    for (const auto& type : meosTypes())
    {
        const auto name = std::string{type.type->name};
        for (const bool nullable : {false, true})
        {
            descriptor.entries.push_back(EntryProvision{
                .registryClassName = "ValueDeserializerRegistry",
                .key = nullable ? "Nullable" + name : name,
                .entry = std::any{ValueDeserializerRegistry::EntryType{
                    [&type, nullable](const ValueDeserializerRegistryArguments arguments)
                    { return std::make_unique<MeosValueDeserializer>(type, nullable, arguments.quoted); }}}});
        }
    }
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
