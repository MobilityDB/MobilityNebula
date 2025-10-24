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

#include <cstdint>
#include <atomic>
#include <memory>
#include <optional>
#include <ostream>
#include <random>
#include <string>
#include <unordered_map>

#include <mqtt/async_client.h>

#include <Configurations/Descriptor.hpp>
#include <Identifiers/Identifiers.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/CSVFormat.hpp>
#include <Util/Logger/Logger.hpp>
#include <PipelineExecutionContext.hpp>

namespace NES::Sinks
{

class MQTTSink : public Sink
{
public:
    static inline std::string NAME = "MQTT";
    explicit MQTTSink(const SinkDescriptor& sinkDescriptor);
    ~MQTTSink() override = default;

    MQTTSink(const MQTTSink&) = delete;
    MQTTSink& operator=(const MQTTSink&) = delete;
    MQTTSink(MQTTSink&&) = delete;
    MQTTSink& operator=(MQTTSink&&) = delete;

    void start(PipelineExecutionContext& pipelineExecutionContext) override;
    void execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext& pipelineExecutionContext) override;
    void stop(PipelineExecutionContext& pipelineExecutionContext) override;

    static Configurations::DescriptorConfig::Config validateAndFormat(std::unordered_map<std::string, std::string> config);

protected:
    std::ostream& toString(std::ostream& str) const override;

private:
    class Callback;

    std::string serverUri;
    std::string clientId;
    std::string topic;
    std::optional<std::string> username;
    std::optional<std::string> password;
    int32_t qos;
    bool cleanSession;
    std::optional<std::string> persistenceDir;
    std::optional<int32_t> maxInflight;
    bool useTls;
    std::optional<std::string> tlsCaCertPath;
    std::optional<std::string> tlsClientCertPath;
    std::optional<std::string> tlsClientKeyPath;
    bool tlsAllowInsecure;

    std::unique_ptr<mqtt::async_client> client;
    std::shared_ptr<Callback> clientCallback;

    std::unique_ptr<Format> formatter;

    static constexpr int32_t DEFAULT_MAX_INFLIGHT_QOS2 = 20;
};

class MQTTSink::Callback : public mqtt::callback
{
public:
    explicit Callback(std::string serverUri);

    void connected(const std::string& cause) override;
    void connection_lost(const std::string& cause) override;
    void delivery_complete(mqtt::delivery_token_ptr token) override;

private:
    std::string targetServerUri;
    std::atomic<uint64_t> deliveredCount{0};
};

namespace detail::uuid
{
static std::random_device rd;
static std::mt19937 gen(rd());
static std::uniform_int_distribution<> dis(0, 15);
static std::uniform_int_distribution<> dis2(8, 11);

std::string generateUUID()
{
    std::stringstream ss;
    ss << std::hex;
    for (int i = 0; i < 8; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 4; i++)
    {
        ss << dis(gen);
    }
    ss << "-4";
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    ss << dis2(gen);
    for (int i = 0; i < 3; i++)
    {
        ss << dis(gen);
    }
    ss << "-";
    for (int i = 0; i < 12; i++)
    {
        ss << dis(gen);
    }
    return ss.str();
}
}

struct ConfigParametersMQTT
{
    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> SERVER_URI{
        "serverURI",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(SERVER_URI, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> CLIENT_ID{
        "clientId",
        "generated",
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string>
        {
            if (auto it = config.find(CLIENT_ID); it != config.end())
            {
                return it->second;
            }
            return detail::uuid::generateUUID();
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TOPIC{
        "topic",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) { return Configurations::DescriptorConfig::tryGet(TOPIC, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> USERNAME{
        "username",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string> {
            if (auto it = config.find("username"); it != config.end() && !it->second.empty()) {
                return it->second;
            }
            return std::nullopt;
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> PASSWORD{
        "password",
        std::nullopt,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<std::string> {
            if (auto it = config.find("password"); it != config.end()) {
                // Allow empty passwords for Belgian Railway (NULL password with username)
                return it->second;
            }
            return std::nullopt;
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<int32_t> QOS{
        "qos",
        1,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<uint8_t>
        {
            // Check if qos is present in config, if not use default value
            if (auto it = config.find("qos"); it != config.end()) {
                int32_t qos = std::stoi(it->second);
                if (qos != 0 && qos != 1 && qos != 2)
                {
                    NES_ERROR("MQTTSink: QualityOfService is: {}, but must be 0, 1, or 2.", qos);
                    return std::nullopt;
                }
                return qos;
            }
            return 1; // Default QOS value
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<bool> CLEAN_SESSION{
        "cleanSession",
        true,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(CLEAN_SESSION, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> PERSISTENCE_DIR{
        "persistenceDir",
        "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(PERSISTENCE_DIR, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<int32_t> MAX_INFLIGHT{
        "maxInflight",
        0,
        [](const std::unordered_map<std::string, std::string>& config) -> std::optional<int32_t>
        {
            if (auto it = config.find("maxInflight"); it != config.end())
            {
                int value = std::stoi(it->second);
                if (value <= 0)
                {
                    NES_ERROR("MQTTSink: maxInflight must be greater than zero when provided, but was {}", value);
                    return std::nullopt;
                }
                return value;
            }
            return 0;
        }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<bool> USE_TLS{
        "useTls",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(USE_TLS, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TLS_CA_CERT{
        "tlsCaCertPath",
        "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(TLS_CA_CERT, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TLS_CLIENT_CERT{
        "tlsClientCertPath",
        "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(TLS_CLIENT_CERT, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<std::string> TLS_CLIENT_KEY{
        "tlsClientKeyPath",
        "",
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(TLS_CLIENT_KEY, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<bool> TLS_ALLOW_INSECURE{
        "tlsAllowInsecure",
        false,
        [](const std::unordered_map<std::string, std::string>& config)
        { return Configurations::DescriptorConfig::tryGet(TLS_ALLOW_INSECURE, config); }};

    static inline const Configurations::DescriptorConfig::ConfigParameter<Configurations::EnumWrapper, Configurations::InputFormat>
        INPUT_FORMAT{
            "inputFormat",
            std::nullopt,
            [](const std::unordered_map<std::string, std::string>& config)
            { return Configurations::DescriptorConfig::tryGet(INPUT_FORMAT, config); }};

    static inline std::unordered_map<std::string, Configurations::DescriptorConfig::ConfigParameterContainer> parameterMap
        = Configurations::DescriptorConfig::createConfigParameterContainerMap(
            SERVER_URI,
            CLIENT_ID,
            QOS,
            TOPIC,
            USERNAME,
            PASSWORD,
            CLEAN_SESSION,
            PERSISTENCE_DIR,
            MAX_INFLIGHT,
            USE_TLS,
            TLS_CA_CERT,
            TLS_CLIENT_CERT,
            TLS_CLIENT_KEY,
            TLS_ALLOW_INSECURE,
            INPUT_FORMAT);
};

}
