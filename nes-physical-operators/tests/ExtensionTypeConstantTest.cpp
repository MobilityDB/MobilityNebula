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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>
#include <tuple>
#include <unordered_map>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/ExtensionType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/FunctionProvider.hpp>
#include <Runtime/Allocator/NesDefaultMemoryAllocator.hpp>
#include <Runtime/BufferManager.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <gtest/gtest.h>
#include <Arena.hpp>
#include <BaseUnitTest.hpp>
#include <ErrorHandling.hpp>
#include <nautilus/std/cstring.h>

namespace NES
{

namespace
{
constexpr uint32_t POOLED_BUFFER_SIZE = 8192;
constexpr uint32_t NUMBER_OF_POOLED_BUFFERS = 1024;
constexpr NES::BufferAlignment BUFFER_ALIGNMENT{64};
constexpr double UNPOOLED_MEMORY_FRACTION = 0.9;
constexpr size_t TOTAL_MEMORY_IN_BYTES = 10 * static_cast<size_t>(NUMBER_OF_POOLED_BUFFERS) * POOLED_BUFFER_SIZE;

/// A type whose text is the hex of its bytes, as a WKB is written
std::vector<std::byte> bytesOfHex(const std::string_view text)
{
    std::vector<std::byte> bytes;
    for (size_t i = 0; i + 1 < text.size(); i += 2)
    {
        bytes.push_back(static_cast<std::byte>(std::stoi(std::string{text.substr(i, 2)}, nullptr, 16)));
    }
    return bytes;
}

constexpr ExtensionType Hex{.name = "TESTHEX", .fromText = bytesOfHex};
constexpr ExtensionType Opaque{.name = "TESTOPAQUE"};

DataType extensionDataType(const ExtensionType& extension)
{
    DataType dataType{DataType::Type::VARSIZED, DataType::NULLABLE::NOT_NULLABLE};
    dataType.extension = &extension;
    return dataType;
}
}

class ExtensionTypeConstantTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ExtensionTypeConstantTest.log", LogLevel::LOG_DEBUG);
        NES_DEBUG("Setup ExtensionTypeConstantTest test class.");
    }

    void SetUp() override
    {
        BaseUnitTest::SetUp();
        bufferManager = BufferManager::create(
            TOTAL_MEMORY_IN_BYTES,
            UNPOOLED_MEMORY_FRACTION,
            BUFFER_ALIGNMENT,
            POOLED_BUFFER_SIZE,
            std::make_shared<NesDefaultMemoryAllocator>());
    }

    std::shared_ptr<AbstractBufferProvider> bufferManager;
    FieldMappingTrait noFields{std::unordered_map<UnqualifiedUnboundField, QualifiedIdentifier>{}};
};

/// A typed constant of a type a plugin adds holds the bytes its text states, not the text
TEST_F(ExtensionTypeConstantTest, HoldsTheBytesItsTextStates)
{
    const auto constant = ConstantValueLogicalFunction{extensionDataType(Hex), "0A0B0C"};
    const auto function = QueryCompilation::FunctionProvider::lowerFunction(constant, noFields);
    Arena arena{bufferManager};
    ArenaRef arenaRef{&arena};
    const auto value = function.execute(Record{}, arenaRef).getRawValueAs<VariableSizedData>();
    std::vector<int8_t> expected{0x0A, 0x0B, 0x0C};
    EXPECT_EQ(value.getSize(), expected.size());
    EXPECT_TRUE(nautilus::memcmp(value.getContent(), expected.data(), expected.size()) == 0);
}

/// A type without a reader of its text has no typed constants
TEST_F(ExtensionTypeConstantTest, RefusedWithoutAReader)
{
    const auto constant = ConstantValueLogicalFunction{extensionDataType(Opaque), "0A0B0C"};
    ASSERT_EXCEPTION_ERRORCODE(std::ignore = QueryCompilation::FunctionProvider::lowerFunction(constant, noFields), ErrorCode::QueryCompilerError);
}

}
