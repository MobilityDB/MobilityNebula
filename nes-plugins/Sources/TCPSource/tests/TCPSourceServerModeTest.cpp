/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <TCPSource.hpp>

#include <DataTypes/DataType.hpp>
#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/BufferManager.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceCatalog.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <gtest/gtest.h>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <optional>
#include <stdexcept>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>

namespace NES::Sources
{
namespace
{
uint16_t findFreePort()
{
    const int socketFd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (socketFd < 0)
    {
        throw std::runtime_error("Unable to create probing socket for test");
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0; /// Let the OS pick a free ephemeral port.

    if (::bind(socketFd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0)
    {
        ::close(socketFd);
        throw std::runtime_error("Unable to bind probing socket for test");
    }

    socklen_t length = sizeof(addr);
    if (::getsockname(socketFd, reinterpret_cast<sockaddr*>(&addr), &length) != 0)
    {
        ::close(socketFd);
        throw std::runtime_error("Unable to query probing socket port for test");
    }

    ::close(socketFd);
    return ntohs(addr.sin_port);
}

int connectWithRetry(uint16_t port, int maxAttempts)
{
    for (int attempt = 0; attempt < maxAttempts; ++attempt)
    {
        const int clientFd = ::socket(AF_INET, SOCK_STREAM, 0);
        if (clientFd < 0)
        {
            continue;
        }

        sockaddr_in target{};
        target.sin_family = AF_INET;
        target.sin_port = htons(port);
        if (::inet_pton(AF_INET, "127.0.0.1", &target.sin_addr) != 1)
        {
            ::close(clientFd);
            continue;
        }

        if (::connect(clientFd, reinterpret_cast<sockaddr*>(&target), sizeof(target)) == 0)
        {
            return clientFd;
        }

        ::close(clientFd);
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    return -1;
}

void closeSocket(int fd)
{
    if (fd >= 0)
    {
        ::close(fd);
    }
}

ParserConfig createDefaultParserConfig()
{
    ParserConfig parserConfig{};
    parserConfig.parserType = "csv";
    parserConfig.tupleDelimiter = "\n";
    parserConfig.fieldDelimiter = ",";
    return parserConfig;
}

} // namespace

TEST(TCPSourceServerModeTest, AcceptsReconnects)
{
    const uint16_t port = findFreePort();

    /// Build a minimal logical source and descriptor.
    Schema schema;
    schema.addField("value", DataType::Type::INT32);

    SourceCatalog catalog;
    const auto logicalSourceOpt = catalog.addLogicalSource("ServerModeLogical", schema);
    ASSERT_TRUE(logicalSourceOpt.has_value());
    const auto& logicalSource = logicalSourceOpt.value();

    std::unordered_map<std::string, std::string> configStrings{
        {"socketHost", "0.0.0.0"},
        {"socketPort", std::to_string(port)},
        {"mode", "server"},
        {"bindAddress", "127.0.0.1"},
        {"listenBacklog", "4"},
        {"tcpKeepalive", "false"},
        {"nodelay", "false"}};

    auto descriptorConfig = TCPSource::validateAndFormat(configStrings);

    auto descriptorOpt = catalog.addPhysicalSource(
        logicalSource,
        NES::INITIAL<WorkerId>,
        TCPSource::name(),
        SourceDescriptor::INVALID_NUMBER_OF_BUFFERS_IN_LOCAL_POOL,
        std::move(descriptorConfig),
        createDefaultParserConfig());

    ASSERT_TRUE(descriptorOpt.has_value());
    auto descriptor = descriptorOpt.value();

    TCPSource source(descriptor);

    std::thread openThread([&source]() { source.open(); });

    const int firstClient = connectWithRetry(port, 40);
    ASSERT_GE(firstClient, 0) << "Failed to connect to TCPSource listener";

    openThread.join();

    auto bufferManager = Memory::BufferManager::create(512, 8);
    auto bufferPoolOpt = bufferManager->createFixedSizeBufferPool(1);
    ASSERT_TRUE(bufferPoolOpt.has_value());
    auto bufferProvider = std::static_pointer_cast<Memory::AbstractBufferProvider>(bufferPoolOpt.value());

    std::stop_source stopSource;
    const auto stopToken = stopSource.get_token();

    const std::string firstPayload = "first-message\n";
    ASSERT_EQ(
        ::send(firstClient, firstPayload.data(), firstPayload.size(), 0),
        static_cast<ssize_t>(firstPayload.size()))
        << "Failed to send first payload";

    auto buffer1 = bufferProvider->getBufferBlocking();
    const auto bytesReadFirst = source.fillTupleBuffer(buffer1, *bufferProvider, stopToken);
    ASSERT_EQ(bytesReadFirst, firstPayload.size());
    std::string receivedFirst(buffer1.getBuffer<char>(), buffer1.getBuffer<char>() + bytesReadFirst);
    EXPECT_EQ(receivedFirst, firstPayload);
    buffer1.release();

    closeSocket(firstClient);

    const std::string secondPayload = "second-message\n";
    std::atomic<bool> client2Connected{false};
    std::atomic<bool> client2Sent{false};
    std::thread secondClientThread([&]() {
        const int secondClient = connectWithRetry(port, 40);
        if (secondClient < 0)
        {
            return;
        }
        client2Connected = true;
        const auto sent = ::send(secondClient, secondPayload.data(), secondPayload.size(), 0);
        if (sent == static_cast<ssize_t>(secondPayload.size()))
        {
            client2Sent = true;
        }
        closeSocket(secondClient);
    });

    auto buffer2 = bufferProvider->getBufferBlocking();
    const auto bytesReadSecond = source.fillTupleBuffer(buffer2, *bufferProvider, stopToken);
    secondClientThread.join();

    ASSERT_TRUE(client2Connected.load()) << "Second client never established connection";
    ASSERT_TRUE(client2Sent.load()) << "Second client failed to send payload";
    ASSERT_EQ(bytesReadSecond, secondPayload.size());
    std::string receivedSecond(buffer2.getBuffer<char>(), buffer2.getBuffer<char>() + bytesReadSecond);
    EXPECT_EQ(receivedSecond, secondPayload);
    buffer2.release();

    stopSource.request_stop();
    source.close();
}

} // namespace NES::Sources
