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

#include <TCPSource.hpp>

#include <cerrno> /// For socket error
#include <chrono>
#include <cstring>
#include <exception>
#include <memory>
#include <ostream>
#include <stop_token>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <sys/select.h>

#include <cstdio>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <unistd.h> /// For read
#include <Configurations/Descriptor.hpp>
#include <DataServer/TCPDataServer.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sources/SourceDescriptor.hpp>
#include <SystestSources/SourceTypes.hpp>
#include <Util/Logger/Logger.hpp>
#include <asm-generic/socket.h>
#include <bits/types/struct_timeval.h>
#include <sys/socket.h> /// For socket functions
#include <ErrorHandling.hpp>
#include <FileDataRegistry.hpp>
#include <InlineDataRegistry.hpp>
#include <SourceRegistry.hpp>
#include <SourceValidationRegistry.hpp>

#include "GeneratorDataRegistry.hpp"


namespace NES::Sources
{

TCPSource::TCPSource(const SourceDescriptor& sourceDescriptor)
    : socketHost(sourceDescriptor.getFromConfig(ConfigParametersTCP::HOST))
    , socketPort(std::to_string(sourceDescriptor.getFromConfig(ConfigParametersTCP::PORT)))
    , socketType(sourceDescriptor.getFromConfig(ConfigParametersTCP::TYPE))
    , socketDomain(sourceDescriptor.getFromConfig(ConfigParametersTCP::DOMAIN))
    , mode(sourceDescriptor.getFromConfig(ConfigParametersTCP::MODE))
    , bindAddress(sourceDescriptor.getFromConfig(ConfigParametersTCP::BIND_ADDRESS))
    , listenBacklog(sourceDescriptor.getFromConfig(ConfigParametersTCP::LISTEN_BACKLOG))
    , tcpKeepalive(sourceDescriptor.getFromConfig(ConfigParametersTCP::TCP_KEEPALIVE))
    , tcpNoDelay(sourceDescriptor.getFromConfig(ConfigParametersTCP::NO_DELAY))
    , tupleDelimiter(sourceDescriptor.getFromConfig(ConfigParametersTCP::SEPARATOR))
    , socketBufferSize(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_SIZE))
    , bytesUsedForSocketBufferSizeTransfer(sourceDescriptor.getFromConfig(ConfigParametersTCP::SOCKET_BUFFER_TRANSFER_SIZE))
    , flushIntervalInMs(sourceDescriptor.getFromConfig(ConfigParametersTCP::FLUSH_INTERVAL_MS))
    , connectionTimeout(sourceDescriptor.getFromConfig(ConfigParametersTCP::CONNECT_TIMEOUT))
{
    /// init physical types
    const std::vector<std::string> schemaKeys;
    NES_TRACE("TCPSource::TCPSource: Init TCPSource.");
}

std::ostream& TCPSource::toString(std::ostream& str) const
{
    const auto originalFlags = str.flags();
    str << std::boolalpha;

    str << "\nTCPSource(";
    str << "\n  mode: " << mode;
    str << "\n  socketHost: " << socketHost;
    str << "\n  socketPort: " << socketPort;
    if (mode == "server")
    {
        str << "\n  bindAddress: " << bindAddress;
        str << "\n  listenBacklog: " << listenBacklog;
        str << "\n  acceptedConnections: " << acceptedConnections;
    }
    str << "\n  peerEndpoint: " << (peerEndpoint.empty() ? std::string("<disconnected>") : peerEndpoint);
    str << "\n  tcpKeepalive: " << tcpKeepalive;
    str << "\n  tcpNoDelay: " << tcpNoDelay;
    str << "\n  timeout: " << connectionTimeout << " seconds";
    str << "\n  socketType: " << socketType;
    str << "\n  socketDomain: " << socketDomain;
    str << "\n  tupleDelimiter: " << tupleDelimiter;
    str << "\n  socketBufferSize: " << socketBufferSize;
    str << "\n  bytesUsedForSocketBufferSizeTransfer: " << bytesUsedForSocketBufferSizeTransfer;
    str << "\n  flushIntervalInMs: " << flushIntervalInMs;
    str << "\n  generated tuples: " << generatedTuples;
    str << "\n  generated buffers: " << generatedBuffers;
    str << "\n  connection state: " << connection;
    str << ")\n";

    str.flags(originalFlags);
    return str;
}

bool TCPSource::tryToConnect(const addrinfo* result, int& originalFlags)
{
    const std::chrono::seconds socketConnectDefaultTimeout{connectionTimeout};

    /// we try each addrinfo until we successfully create a socket
    const addrinfo* selectedAddress = result;
    while (selectedAddress != nullptr)
    {
        sockfd = socket(selectedAddress->ai_family, selectedAddress->ai_socktype, selectedAddress->ai_protocol);

        if (sockfd != -1)
        {
            break;
        }
        selectedAddress = selectedAddress->ai_next;
    }

    /// check if we found a vaild address
    if (selectedAddress == nullptr)
    {
        NES_ERROR("No valid address found to create socket.");
        return false;
    }

    originalFlags = fcntl(sockfd, F_GETFL, 0);
    if (originalFlags == -1)
    {
        /// Default to blocking mode if we fail to read current flags
        originalFlags = 0;
    }

    fcntl(sockfd, F_SETFL, originalFlags | O_NONBLOCK);

    /// set timeout for both blocking receive and send calls
    /// if timeout is set to zero, then the operation will never timeout
    /// (https://linux.die.net/man/7/socket)
    /// as a workaround, we implicitly add one microsecond to the timeout
    timeval timeout{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
    connection = connect(sockfd, selectedAddress->ai_addr, selectedAddress->ai_addrlen);

    /// if the TCPSource did not establish a connection, try with timeout
    if (connection < 0)
    {
        if (errno != EINPROGRESS)
        {
            close();
            /// if connection was unsuccessful, throw an exception with context using errno
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }

        /// Set the timeout for the connect attempt
        fd_set fdset;
        timeval timeValue{.tv_sec = socketConnectDefaultTimeout.count(), .tv_usec = IMPLICIT_TIMEOUT_USEC};

        FD_ZERO(&fdset);
        FD_SET(sockfd, &fdset);

        connection = select(sockfd + 1, nullptr, &fdset, nullptr, &timeValue);
        if (connection <= 0)
        {
            /// Timeout or error
            errno = ETIMEDOUT;
            close();
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }

        /// Check if connect succeeded
        int error = 0;
        socklen_t len = sizeof(error);
        if (getsockopt(sockfd, SOL_SOCKET, SO_ERROR, &error, &len) < 0 || (error != 0))
        {
            errno = error;
            close();
            strerror_r(errno, errBuffer.data(), errBuffer.size());
            throw CannotOpenSource("Could not connect to: {}:{}. {}", socketHost, socketPort, errBuffer.data());
        }
    }
    return true;
}

void TCPSource::configureSocketOptions(const int fd) const
{
    timeval timeout{
        .tv_sec = static_cast<time_t>(connectionTimeout),
        .tv_usec = IMPLICIT_TIMEOUT_USEC,
    };

    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        NES_WARNING("TCPSource::configureSocketOptions: Failed to set SO_RCVTIMEO. errno: {}", errno);
    }
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout)) < 0)
    {
        NES_WARNING("TCPSource::configureSocketOptions: Failed to set SO_SNDTIMEO. errno: {}", errno);
    }

    if (tcpKeepalive)
    {
        const int keepAliveValue = 1;
        if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &keepAliveValue, sizeof(keepAliveValue)) < 0)
        {
            NES_WARNING("TCPSource::configureSocketOptions: Failed to enable SO_KEEPALIVE. errno: {}", errno);
        }
    }

#ifdef TCP_NODELAY
    if (tcpNoDelay)
    {
        const int noDelayValue = 1;
        if (setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &noDelayValue, sizeof(noDelayValue)) < 0)
        {
        NES_WARNING("TCPSource::configureSocketOptions: Failed to enable TCP_NODELAY. errno: {}", errno);
        }
    }
#endif
}

std::string TCPSource::formatEndpoint(const sockaddr_storage& address, const socklen_t length) const
{
    char host[NI_MAXHOST] = {0};
    char service[NI_MAXSERV] = {0};

    const int result = getnameinfo(
        reinterpret_cast<const sockaddr*>(&address),
        length,
        host,
        sizeof(host),
        service,
        sizeof(service),
        NI_NUMERICHOST | NI_NUMERICSERV);

    if (result == 0)
    {
        std::string hostString(host);
        std::string serviceString(service);
        const bool hostIsIpv6 = hostString.find(':') != std::string::npos;
        if (hostIsIpv6)
        {
            return "[" + hostString + "]:" + serviceString;
        }
        return hostString + ":" + serviceString;
    }

    NES_WARNING("TCPSource::formatEndpoint: getnameinfo failed with error code {}", result);
    return "<unknown>";
}

void TCPSource::updatePeerEndpointFromSocket()
{
    peerEndpoint.clear();

    if (sockfd < 0)
    {
        return;
    }

    sockaddr_storage peerAddress{};
    socklen_t peerAddressLength = sizeof(peerAddress);
    if (getpeername(sockfd, reinterpret_cast<sockaddr*>(&peerAddress), &peerAddressLength) == 0)
    {
        peerEndpoint = formatEndpoint(peerAddress, peerAddressLength);
        return;
    }

    NES_WARNING("TCPSource::updatePeerEndpointFromSocket: getpeername failed. errno: {}", errno);
    peerEndpoint = socketHost + ":" + socketPort;
}

void TCPSource::openClientConnection()
{
    peerEndpoint.clear();

    if (sockfd >= 0)
    {
        ::close(sockfd);
        sockfd = -1;
    }

    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = 0; /// use default behavior
    hints.ai_protocol
        = 0; /// specifying 0 in this field indicates that socket addresses with any protocol can be returned by getaddrinfo()

    const auto errorCode = getaddrinfo(socketHost.c_str(), socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("Failed getaddrinfo with error: {}", gai_strerror(errorCode));
    }

    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    int originalFlags = 0;
    try
    {
        tryToConnect(result, originalFlags);
    }
    catch (const std::exception& e)
    {
        if (sockfd >= 0)
        {
            ::close(sockfd);
            sockfd = -1;
        }
        peerEndpoint.clear();
        throw CannotOpenSource("Could not establish connection! Error: {}", e.what());
    }

    if (fcntl(sockfd, F_SETFL, originalFlags) == -1)
    {
        NES_WARNING("TCPSource::openClientConnection: Failed to restore socket flags. errno: {}", errno);
    }

    configureSocketOptions(sockfd);
    connection = 0;
    updatePeerEndpointFromSocket();

    NES_INFO("TCPSource::openClientConnection: Connected to {}.", peerEndpoint);
}

void TCPSource::setupServerListener()
{
    addrinfo hints{};
    addrinfo* result = nullptr;

    hints.ai_family = socketDomain;
    hints.ai_socktype = socketType;
    hints.ai_flags = AI_PASSIVE;
    hints.ai_protocol = 0;

    const char* bindAddressPointer = bindAddress.empty() ? nullptr : bindAddress.c_str();
    const auto errorCode = getaddrinfo(bindAddressPointer, socketPort.c_str(), &hints, &result);
    if (errorCode != 0)
    {
        throw CannotOpenSource("Failed getaddrinfo for bind address with error: {}", gai_strerror(errorCode));
    }

    const std::unique_ptr<addrinfo, decltype(&freeaddrinfo)> resultGuard(result, freeaddrinfo);

    listenfd = -1;
    for (addrinfo* entry = result; entry != nullptr; entry = entry->ai_next)
    {
        listenfd = socket(entry->ai_family, entry->ai_socktype, entry->ai_protocol);
        if (listenfd == -1)
        {
            continue;
        }

        const int reuseAddress = 1;
        if (setsockopt(listenfd, SOL_SOCKET, SO_REUSEADDR, &reuseAddress, sizeof(reuseAddress)) < 0)
        {
            NES_WARNING("TCPSource::setupServerListener: Failed to set SO_REUSEADDR. errno: {}", errno);
        }

        if (bind(listenfd, entry->ai_addr, entry->ai_addrlen) == 0)
        {
            if (listen(listenfd, static_cast<int>(listenBacklog)) == 0)
            {
                break;
            }
            NES_WARNING("TCPSource::setupServerListener: listen() failed. errno: {}", errno);
        }
        else
        {
            NES_WARNING("TCPSource::setupServerListener: bind() failed. errno: {}", errno);
        }

        ::close(listenfd);
        listenfd = -1;
    }

    if (listenfd == -1)
    {
        throw CannotOpenSource("Failed to create listening socket on {}:{}", bindAddress.empty() ? "0.0.0.0" : bindAddress, socketPort);
    }

    NES_INFO(
        "TCPSource::setupServerListener: Listening on {}:{} with backlog {}.",
        bindAddress.empty() ? "0.0.0.0" : bindAddress,
        socketPort,
        listenBacklog);

    /// Ensure the previous connection descriptor is cleared; accept happens on demand.
    if (sockfd >= 0)
    {
        ::close(sockfd);
        sockfd = -1;
    }
    acceptedConnections = 0;
    peerEndpoint.clear();
}

void TCPSource::awaitClientConnection()
{
    if (listenfd < 0)
    {
        throw CannotOpenSource("TCPSource::awaitClientConnection called without active listening socket.");
    }

    while (true)
    {
        sockaddr_storage clientAddress{};
        socklen_t clientAddressLength = sizeof(clientAddress);
        const int acceptedFd = accept(listenfd, reinterpret_cast<sockaddr*>(&clientAddress), &clientAddressLength);

        if (acceptedFd >= 0)
        {
            sockfd = acceptedFd;
            configureSocketOptions(sockfd);
            connection = 0;
            peerEndpoint = formatEndpoint(clientAddress, clientAddressLength);
            ++acceptedConnections;
            NES_INFO("TCPSource::awaitClientConnection: Accepted client {} (total: {}).", peerEndpoint, acceptedConnections);
            break;
        }

        if (errno == EINTR)
        {
            continue;
        }

        strerror_r(errno, errBuffer.data(), errBuffer.size());
        ::close(listenfd);
        listenfd = -1;
        peerEndpoint.clear();
        throw CannotOpenSource(
            "Failed to accept connection on {}:{}. {}",
            bindAddress.empty() ? "0.0.0.0" : bindAddress,
            socketPort,
            errBuffer.data());
    }
}

void TCPSource::open()
{
    NES_TRACE("TCPSource::open: Initializing TCP connection in {} mode.", mode);

    if (mode == "server")
    {
        setupServerListener();
        awaitClientConnection();
    }
    else
    {
        openClientConnection();
    }
}

size_t TCPSource::fillTupleBuffer(NES::Memory::TupleBuffer& tupleBuffer, Memory::AbstractBufferProvider&, const std::stop_token&)
{
    try
    {
        size_t numReceivedBytes = 0;
        while (fillBuffer(tupleBuffer, numReceivedBytes))
        {
            /// Fill the buffer until EoS reached or the number of tuples in the buffer is not equals to 0.
        };
        return numReceivedBytes;
    }
    catch (const std::exception& e)
    {
        NES_ERROR("TCPSource::receiveData: Failed to fill the TupleBuffer. Error: {}.", e.what());
        throw e;
    }
}

bool TCPSource::fillBuffer(NES::Memory::TupleBuffer& tupleBuffer, size_t& numReceivedBytes)
{
    if (sockfd < 0)
    {
        if (mode == "server")
        {
            awaitClientConnection();
        }
        else
        {
            openClientConnection();
        }
    }

    const auto flushIntervalTimerStart = std::chrono::system_clock::now();
    bool flushIntervalPassed = false;
    bool readWasValid = true;

    const size_t rawTBSize = tupleBuffer.getBufferSize();
    while (not flushIntervalPassed and numReceivedBytes < rawTBSize)
    {
        const ssize_t bufferSizeReceived = read(sockfd, tupleBuffer.getBuffer() + numReceivedBytes, rawTBSize - numReceivedBytes);
        numReceivedBytes += bufferSizeReceived;
        if (bufferSizeReceived == INVALID_RECEIVED_BUFFER_SIZE)
        {
            /// if read method returned -1 an error occurred during read.
            NES_ERROR("An error occurred while reading from socket. Error: {}", strerror(errno));
            readWasValid = false;
            numReceivedBytes = 0;
            break;
        }
        if (bufferSizeReceived == EOF_RECEIVED_BUFFER_SIZE)
        {
            NES_TRACE("No data received from {}:{}.", socketHost, socketPort);
            if (numReceivedBytes == 0)
            {
                NES_INFO("TCPSource::fillBuffer: detected EoS");
                readWasValid = false;
                if (mode == "server")
                {
                    NES_INFO(
                        "TCPSource::fillBuffer: peer {} disconnected, awaiting next client.",
                        peerEndpoint.empty() ? std::string("<unknown>") : peerEndpoint);
                    if (sockfd >= 0)
                    {
                        ::close(sockfd);
                        sockfd = -1;
                    }
                    peerEndpoint.clear();
                    NES_DEBUG("TCPSource::fillBuffer: waiting for new client connection after EOF in server mode.");
                    awaitClientConnection();
                    readWasValid = true;
                    continue;
                }
                break;
            }
        }
        /// If bufferFlushIntervalMs was defined by the user (> 0), we check whether the time on receiving
        /// and writing data exceeds the user defined limit (bufferFlushIntervalMs).
        /// If so, we flush the current TupleBuffer(TB) and proceed with the next TB.
        if ((flushIntervalInMs > 0
             && std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::system_clock::now() - flushIntervalTimerStart).count()
                 >= flushIntervalInMs))
        {
            NES_DEBUG("TCPSource::fillBuffer: Reached TupleBuffer flush interval. Finishing writing to current TupleBuffer.");
            flushIntervalPassed = true;
        }
    }
    ++generatedBuffers;
    /// Loop while we haven't received any bytes yet and we can still read from the socket.
    return numReceivedBytes == 0 and readWasValid;
}

NES::Configurations::DescriptorConfig::Config TCPSource::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    return NES::Configurations::DescriptorConfig::validateAndFormat<ConfigParametersTCP>(std::move(config), name());
}

void TCPSource::close()
{
    NES_DEBUG("TCPSource::close: trying to close connection.");
    if (sockfd >= 0)
    {
        ::close(sockfd);
        sockfd = -1;
        connection = -1;
        NES_TRACE("TCPSource::close: connection socket closed.");
    }
    if (listenfd >= 0)
    {
        ::close(listenfd);
        listenfd = -1;
        NES_TRACE("TCPSource::close: listening socket closed.");
    }
    peerEndpoint.clear();
}

SourceValidationRegistryReturnType
SourceValidationGeneratedRegistrar::RegisterTCPSourceValidation(SourceValidationRegistryArguments sourceConfig)
{
    return TCPSource::validateAndFormat(std::move(sourceConfig.config));
}

SourceRegistryReturnType SourceGeneratedRegistrar::RegisterTCPSource(SourceRegistryArguments sourceRegistryArguments)
{
    return std::make_unique<TCPSource>(sourceRegistryArguments.sourceDescriptor);
}

InlineDataRegistryReturnType InlineDataGeneratedRegistrar::RegisterTCPInlineData(InlineDataRegistryArguments systestAdaptorArguments)
{
    if (systestAdaptorArguments.attachSource.tuples)
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersTCP::PORT);
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<TCPDataServer>(std::move(systestAdaptorArguments.attachSource.tuples.value()));
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersTCP::HOST);
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw TestException("An INLINE SystestAttachSource must not have a 'tuples' vector that is null.");
}

FileDataRegistryReturnType FileDataGeneratedRegistrar::RegisterTCPFileData(FileDataRegistryArguments systestAdaptorArguments)
{
    if (const auto attachSourceFilePath = systestAdaptorArguments.attachSource.fileDataPath)
    {
        if (const auto port = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersTCP::PORT);
            port != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
        {
            auto mockTCPServer = std::make_unique<TCPDataServer>(attachSourceFilePath.value());
            port->second = std::to_string(mockTCPServer->getPort());

            if (const auto host = systestAdaptorArguments.physicalSourceConfig.sourceConfig.find(ConfigParametersTCP::HOST);
                host != systestAdaptorArguments.physicalSourceConfig.sourceConfig.end())
            {
                host->second = "localhost";
                auto serverThread
                    = std::jthread([server = std::move(mockTCPServer)](const std::stop_token& stopToken) { server->run(stopToken); });
                systestAdaptorArguments.attachSource.serverThreads->push_back(std::move(serverThread));

                systestAdaptorArguments.physicalSourceConfig.sourceConfig.erase(std::string(SYSTEST_FILE_PATH_PARAMETER));
                return systestAdaptorArguments.physicalSourceConfig;
            }
            throw InvalidConfigParameter("A TCP source config must contain a 'host' parameter");
        }
        throw InvalidConfigParameter("A TCP source config must contain a 'port' parameter");
    }
    throw InvalidConfigParameter("An attach source of type FileData must contain a filePath configuration.");
}

///NOLINTNEXTLINE (performance-unnecessary-value-param)
GeneratorDataRegistryReturnType
GeneratorDataGeneratedRegistrar::RegisterTCPGeneratorData(GeneratorDataRegistryArguments systestAdaptorArguments)
{
    return systestAdaptorArguments.physicalSourceConfig;
}
}
#include <Util/Logger/Logger.hpp>
