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

#include <functional>
#include <string>
#include <tuple>

#include <DataTypes/DataType.hpp>
#include <DataTypes/DataTypeProvider.hpp>
#include <DataTypes/ExtensionType.hpp>
#include <Util/Logger/LogLevel.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>
#include <Util/Reflection.hpp>
#include <fmt/format.h>
#include <gtest/gtest.h>
#include <BaseUnitTest.hpp>
#include <DataTypeRegistry.hpp>
#include <ErrorHandling.hpp>

namespace NES
{

namespace
{
/// Two types a plugin could add over VARSIZED
constexpr ExtensionType Point{.name = "TESTPOINT"};
constexpr ExtensionType Box{.name = "TESTBOX"};

DataType extensionDataType(const ExtensionType& extension, const DataType::NULLABLE nullable)
{
    DataType dataType{DataType::Type::VARSIZED, nullable};
    dataType.extension = &extension;
    return dataType;
}
}

class ExtensionTypeTest : public Testing::BaseUnitTest
{
public:
    static void SetUpTestSuite()
    {
        Logger::setupLogging("ExtensionTypeTest.log", LogLevel::LOG_DEBUG);
        NES_INFO("ExtensionTypeTest test class SetUpTestCase.");
        for (const auto* extension : {&Point, &Box})
        {
            /// A second registration of the same name, by a rerun of the suite in one process, keeps the first
            [[maybe_unused]] const bool added = DataTypeRegistry::instance().addEntry(
                std::string{extension->name},
                [extension](const DataTypeRegistryArguments arguments) { return extensionDataType(*extension, arguments.nullable); });
        }
    }
};

/// A schema names the type regardless of case and gets the VARSIZED field carrying it
TEST_F(ExtensionTypeTest, ProvidedByName)
{
    const auto dataType = DataTypeProvider::provideDataType("testpoint", DataType::NULLABLE::IS_NULLABLE);
    EXPECT_EQ(dataType.type, DataType::Type::VARSIZED);
    EXPECT_TRUE(dataType.nullable);
    EXPECT_EQ(dataType.extension, &Point);
    EXPECT_TRUE(dataType.isExtension());
    EXPECT_FALSE(DataTypeProvider::provideDataType(DataType::Type::VARSIZED).isExtension());
}

/// Two fields have the same type exactly when they carry the same extension
TEST_F(ExtensionTypeTest, EqualityAndHash)
{
    const auto point = DataTypeProvider::provideDataType("TESTPOINT");
    const auto box = DataTypeProvider::provideDataType("TESTBOX");
    const auto varsized = DataTypeProvider::provideDataType(DataType::Type::VARSIZED);
    EXPECT_EQ(point, DataTypeProvider::provideDataType("TESTPOINT"));
    EXPECT_NE(point, box);
    EXPECT_NE(point, varsized);
    const std::hash<DataType> hash{};
    EXPECT_EQ(hash(point), hash(DataTypeProvider::provideDataType("TESTPOINT")));
    EXPECT_NE(hash(point), hash(varsized));
}

/// A type a plugin adds joins only with itself, and the join keeps it
TEST_F(ExtensionTypeTest, Join)
{
    const auto point = DataTypeProvider::provideDataType("TESTPOINT", DataType::NULLABLE::NOT_NULLABLE);
    const auto nullablePoint = DataTypeProvider::provideDataType("TESTPOINT", DataType::NULLABLE::IS_NULLABLE);
    const auto joined = point.join(nullablePoint);
    ASSERT_TRUE(joined.has_value());
    EXPECT_EQ(*joined, nullablePoint);
    EXPECT_FALSE(point.join(DataTypeProvider::provideDataType("TESTBOX")).has_value());
    EXPECT_FALSE(point.join(DataTypeProvider::provideDataType(DataType::Type::VARSIZED)).has_value());
    EXPECT_FALSE(DataTypeProvider::provideDataType(DataType::Type::VARSIZED).join(point).has_value());
    EXPECT_FALSE(point.join(DataTypeProvider::provideDataType(DataType::Type::INT32)).has_value());
}

/// A field of the type is printed by its name
TEST_F(ExtensionTypeTest, Printed)
{
    EXPECT_EQ(
        fmt::format("{}", DataTypeProvider::provideDataType("TESTPOINT")), "DataType(type: VARSIZED extension: TESTPOINT nullable: false)");
}

/// Reflection writes the name and reads the type back through the DataTypeRegistry
TEST_F(ExtensionTypeTest, ReflectionRoundTrip)
{
    const ReflectionContext context{};
    for (const auto nullable : {DataType::NULLABLE::NOT_NULLABLE, DataType::NULLABLE::IS_NULLABLE})
    {
        const auto point = DataTypeProvider::provideDataType("TESTPOINT", nullable);
        EXPECT_EQ(context.unreflect<DataType>(context.reflect(point)), point);
    }
    const auto int32 = DataTypeProvider::provideDataType(DataType::Type::INT32);
    EXPECT_EQ(context.unreflect<DataType>(context.reflect(int32)), int32);
}

/// A name no plugin registered is refused when read back
TEST_F(ExtensionTypeTest, ReflectionRefusesAnUnknownName)
{
    static constexpr ExtensionType Unknown{.name = "TESTUNKNOWN"};
    const ReflectionContext context{};
    const auto reflected = context.reflect(extensionDataType(Unknown, DataType::NULLABLE::NOT_NULLABLE));
    ASSERT_EXCEPTION_ERRORCODE(std::ignore = context.unreflect<DataType>(reflected), ErrorCode::CannotDeserialize);
}

}
