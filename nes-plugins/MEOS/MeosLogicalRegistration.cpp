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

/// The MEOS types and functions registered with the logical operators: each type as the DataType carrying it
/// and the ValueSerializer writing its text, each function as the logical function of its SQL name and its
/// unreflection under every name its type is reflected as. The entries are described to the plugin loader as
/// the glue of an add_registry_entry describes its one entry (cmake/RuntimeRegistrationUtil.cmake).

#include <any>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <DataTypes/DataType.hpp>
#include <DataTypes/ExtensionType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/MeosLogicalFunction.hpp>
#include <Interface/RecordBuffer.hpp>
#include <OutputFormatters/OutputFormatterUtil.hpp>
#include <OutputFormatters/ValueSerializer.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <Plugins/PluginDescriptor.hpp>
#include <Runtime/AbstractBufferProvider.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Reflection.hpp>
#include <DataTypeRegistry.hpp>
#include <LogicalFunctionRegistry.hpp>
#include <LogicalFunctionUnreflectionRegistry.hpp>
#include <MeosRegistration.hpp>
#include <ValueSerializerRegistry.hpp>
#include <function.hpp>
#include <val_arith.hpp>
#include <val_ptr.hpp>

namespace NES
{

namespace
{

/// The text of a MEOS value written where the formatter writes the field, between double quotes with its
/// double quotes and backslashes escaped when the formatter quotes its strings, as JSON does
uint64_t writeText(
    const MeosType* type,
    const int8_t* content,
    const uint64_t size,
    const bool quoted,
    int8_t* startingAddress,
    const uint64_t remainingSpace,
    TupleBuffer* tupleBuffer,
    AbstractBufferProvider* bufferProvider)
{
    auto text = type->toText(std::as_bytes(std::span{content, size}));
    if (quoted)
    {
        std::string escaped{"\""};
        for (const char c : text)
        {
            if (c == '"' or c == '\\')
            {
                escaped += '\\';
            }
            escaped += c;
        }
        escaped += '"';
        text = std::move(escaped);
    }
    return writeValueToBuffer(text.data(), text.size(), remainingSpace, tupleBuffer, bufferProvider, startingAddress);
}

/// The ValueSerializer of a MEOS type, writing the text of the value its bytes carry
class MeosValueSerializer final : public ValueSerializer
{
public:
    MeosValueSerializer(const MeosType& type, const bool quoted) : type(&type), quoted(quoted) { }

    [[nodiscard]] nautilus::val<uint64_t> serializeAndWrite(
        const VarVal& value,
        const nautilus::val<uint64_t>& remainingSize,
        const RecordBuffer& recordBuffer,
        const nautilus::val<AbstractBufferProvider*>& bufferProvider,
        const nautilus::val<int8_t*>& startingAddress) const override
    {
        const auto bytes = value.getRawValueAs<VariableSizedData>();
        return nautilus::invoke(
            {.modRefInfo = nautilus::ModRefInfo::ModRef, .noUnwind = false},
            writeText,
            nautilus::val<const MeosType*>{type},
            bytes.getContent(),
            bytes.getSize(),
            nautilus::val<bool>{quoted},
            startingAddress,
            remainingSize,
            recordBuffer.getReference(),
            bufferProvider);
    }

private:
    const MeosType* type;
    bool quoted;
};

void describe(PluginDescriptor& descriptor)
{
    for (const auto& type : meosTypes())
    {
        const auto name = std::string{type.type->name};
        descriptor.entries.push_back(EntryProvision{
            .registryClassName = "DataTypeRegistry",
            .key = name,
            .entry = std::any{DataTypeRegistry::EntryType{[extension = type.type](const DataTypeRegistryArguments arguments)
                                                          {
                                                              DataType dataType{DataType::Type::VARSIZED, arguments.nullable};
                                                              dataType.extension = extension;
                                                              return dataType;
                                                          }}}});
        descriptor.entries.push_back(EntryProvision{
            .registryClassName = "ValueSerializerRegistry",
            .key = name,
            .entry = std::any{ValueSerializerRegistry::EntryType{[&type](const ValueSerializerRegistryArguments arguments)
                                                                 { return std::make_unique<MeosValueSerializer>(type, arguments.quoted); }}}});
    }

    const auto unreflect = [](const Reflected& data, const ReflectionContext& context) -> LogicalFunction
    { return context.unreflect<MeosLogicalFunction>(data); };
    for (const auto& function : meosFunctions())
    {
        descriptor.entries.push_back(EntryProvision{
            .registryClassName = "LogicalFunctionRegistry",
            .key = std::string{function.name},
            .entry = std::any{LogicalFunctionRegistry::EntryType{[function](LogicalFunctionRegistryArguments arguments) -> LogicalFunction
                                                                 { return MeosLogicalFunction{function.name, function.overloads, std::move(arguments.children)}; }}}});
        /// A function is reflected as its SQL name until its signature is chosen, then as the signature's physical function
        std::set<std::string_view> reflectedAs{function.name};
        for (const auto& overload : function.overloads)
        {
            reflectedAs.insert(overload.physicalFunction);
        }
        for (const auto key : reflectedAs)
        {
            descriptor.entries.push_back(EntryProvision{
                .registryClassName = "LogicalFunctionUnreflectionRegistry",
                .key = std::string{key},
                .entry = std::any{LogicalFunctionUnreflectionRegistry::EntryType{unreflect}}});
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
