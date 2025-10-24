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

#include <MQTTSink.hpp>

#include <memory>
#include <filesystem>
#include <ostream>
#include <string>
#include <unordered_map>
#include <utility>

#include <fmt/format.h>
#include <mqtt/async_client.h>
#include <mqtt/exception.h>
#include <mqtt/message.h>

#include <Configurations/Descriptor.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Sinks/Sink.hpp>
#include <Sinks/SinkDescriptor.hpp>
#include <SinksParsing/JSONFormat.hpp>
#include <ErrorHandling.hpp>
#include <PipelineExecutionContext.hpp>
#include <SinkRegistry.hpp>
#include <SinkValidationRegistry.hpp>

namespace NES::Sinks
{

MQTTSink::Callback::Callback(std::string serverUri) : targetServerUri(std::move(serverUri))
{
}

void MQTTSink::Callback::connected(const std::string& cause)
{
    NES_INFO("MQTTSink: Connected to {}{}.", targetServerUri, cause.empty() ? "" : fmt::format(" (cause: {})", cause));
}

void MQTTSink::Callback::connection_lost(const std::string& cause)
{
    NES_WARNING("MQTTSink: Connection to {} lost (cause: {}).", targetServerUri, cause.empty() ? "<unknown>" : cause);
}

void MQTTSink::Callback::delivery_complete(mqtt::delivery_token_ptr token)
{
    const auto count = ++deliveredCount;
    if (token)
    {
        NES_DEBUG(
            "MQTTSink: delivery {} completed (token id: {}, message size: {}).",
            count,
            token->get_message_id(),
            token->get_message() ? token->get_message()->to_string().size() : 0);
    }
    else
    {
        NES_DEBUG("MQTTSink: delivery {} completed (token unavailable).", count);
    }
}

MQTTSink::MQTTSink(const SinkDescriptor& sinkDescriptor)
    : Sink()
    , serverUri(sinkDescriptor.getFromConfig(ConfigParametersMQTT::SERVER_URI))
    , clientId(sinkDescriptor.getFromConfig(ConfigParametersMQTT::CLIENT_ID))
    , topic(sinkDescriptor.getFromConfig(ConfigParametersMQTT::TOPIC))
    , username(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::USERNAME))
    , password(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::PASSWORD))
    , qos(sinkDescriptor.getFromConfig(ConfigParametersMQTT::QOS))
    , cleanSession(sinkDescriptor.getFromConfig(ConfigParametersMQTT::CLEAN_SESSION))
    , persistenceDir(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::PERSISTENCE_DIR))
    , maxInflight(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::MAX_INFLIGHT))
    , useTls(sinkDescriptor.getFromConfig(ConfigParametersMQTT::USE_TLS))
    , tlsCaCertPath(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::TLS_CA_CERT))
    , tlsClientCertPath(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::TLS_CLIENT_CERT))
    , tlsClientKeyPath(sinkDescriptor.tryGetFromConfig(ConfigParametersMQTT::TLS_CLIENT_KEY))
    , tlsAllowInsecure(sinkDescriptor.getFromConfig(ConfigParametersMQTT::TLS_ALLOW_INSECURE))
{
    switch (const auto inputFormat = sinkDescriptor.getFromConfig(ConfigParametersMQTT::INPUT_FORMAT))
    {
        case Configurations::InputFormat::CSV:
            formatter = std::make_unique<CSVFormat>(sinkDescriptor.schema);
            break;
        case Configurations::InputFormat::JSON:
            formatter = std::make_unique<JSONFormat>(sinkDescriptor.schema);
            break;
        default:
            throw UnknownSinkFormat(fmt::format("Sink format: {} not supported.", magic_enum::enum_name(inputFormat)));
    }
}

std::ostream& MQTTSink::toString(std::ostream& str) const
{
    str << fmt::format("MQTTSink(serverURI: {}, clientId: {}, topic: {}, qos: {})", serverUri, clientId, topic, qos);
    return str;
}

void MQTTSink::start(PipelineExecutionContext&)
{
    if (persistenceDir.has_value() && !persistenceDir->empty())
    {
        std::error_code ec;
        std::filesystem::create_directories(*persistenceDir, ec);
        if (ec)
        {
            NES_WARNING("MQTTSink: Failed creating persistence directory '{}': {}", *persistenceDir, ec.message());
        }
        client = std::make_unique<mqtt::async_client>(serverUri, clientId, *persistenceDir);
    }
    else
    {
        client = std::make_unique<mqtt::async_client>(serverUri, clientId);
    }

    try
    {
        const bool effectiveCleanSession = qos == 2 ? false : cleanSession;
        if (qos == 2 && cleanSession)
        {
            NES_WARNING("MQTTSink: Overriding cleanSession=true to false for QoS2 to ensure persistent session completion.");
        }
        auto optionsBuilder = mqtt::connect_options_builder()
            .automatic_reconnect(true)
            .clean_session(effectiveCleanSession);

        const int32_t configuredMaxInflight = maxInflight.value_or(0);
        if (configuredMaxInflight > 0)
        {
            optionsBuilder.max_inflight(configuredMaxInflight);
        }
        else if (qos == 2)
        {
            optionsBuilder.max_inflight(DEFAULT_MAX_INFLIGHT_QOS2);
            NES_INFO(
                "MQTTSink: QoS2 enabled without 'maxInflight'; applying default {} inflight messages.",
                DEFAULT_MAX_INFLIGHT_QOS2);
        }

        // Add authentication if username is provided
        if (username.has_value() && !username->empty()) {
            optionsBuilder.user_name(*username);
            if (password.has_value()) {
                optionsBuilder.password(*password);
            }
        }

        if (useTls)
        {
            auto sslBuilder = mqtt::ssl_options_builder();
            if (tlsCaCertPath && !tlsCaCertPath->empty())
            {
                sslBuilder.trust_store(*tlsCaCertPath);
            }
            if (tlsClientCertPath && !tlsClientCertPath->empty())
            {
                sslBuilder.key_store(*tlsClientCertPath);
            }
            if (tlsClientKeyPath && !tlsClientKeyPath->empty())
            {
                sslBuilder.private_key(*tlsClientKeyPath);
            }
            sslBuilder.enable_server_cert_auth(!tlsAllowInsecure);
            optionsBuilder.ssl(sslBuilder.finalize());
        }

        const auto connectOptions = optionsBuilder.finalize();

        clientCallback = std::make_shared<Callback>(serverUri);
        client->set_callback(*clientCallback);
        client->connect(connectOptions)->wait();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink(e.what());
    }
}

void MQTTSink::stop(PipelineExecutionContext&)
{
    try
    {
        client->disconnect()->wait();
        clientCallback.reset();
    }
    catch (const mqtt::exception& e)
    {
        throw CannotOpenSink("When closing mqtt sink: {}", e.what());
    }
}

void MQTTSink::execute(const Memory::TupleBuffer& inputBuffer, PipelineExecutionContext&)
{
    if (inputBuffer.getNumberOfTuples() == 0)
    {
        return;
    }

    // Check connection state before attempting to publish
    if (!client->is_connected())
    {
        throw CannotOpenSink("MQTT client is not connected to server {}", serverUri);
    }

    const std::string fBuf = formatter->getFormattedBuffer(inputBuffer);
    const mqtt::message_ptr message = mqtt::make_message(topic, fBuf);
    message->set_qos(qos);

    try
    {
        auto token = client->publish(message);
        // Only wait for acknowledgment if QoS > 0
        // QoS 0 is fire-and-forget and doesn't require acknowledgment
        if (qos > 0)
        {
            token->wait();
        }
    }
    catch (const mqtt::exception& e)
    {
        // Handle specific MQTT exceptions with error code
        throw CannotOpenSink("MQTT publish failed with error [{}]: {}", e.get_reason_code(), e.what());
    }
    catch (const std::exception& e)
    {
        throw CannotOpenSink("Failed to publish to MQTT: {}", e.what());
    }
    catch (...)
    {
        throw wrapExternalException();
    }
}

Configurations::DescriptorConfig::Config MQTTSink::validateAndFormat(std::unordered_map<std::string, std::string> config)
{
    const bool cleanSessionProvided = config.contains(std::string(ConfigParametersMQTT::CLEAN_SESSION));
    auto validated = Configurations::DescriptorConfig::validateAndFormat<ConfigParametersMQTT>(std::move(config), NAME);

    if (!cleanSessionProvided)
    {
        const auto qosIt = validated.find(std::string(ConfigParametersMQTT::QOS));
        if (qosIt != validated.end())
        {
            const auto qosValue = std::get<int32_t>(qosIt->second);
            if (qosValue == 2)
            {
                validated[std::string(ConfigParametersMQTT::CLEAN_SESSION)] = false;
            }
        }
    }

    return validated;
}

SinkValidationRegistryReturnType SinkValidationGeneratedRegistrar::RegisterMQTTSinkValidation(SinkValidationRegistryArguments sinkConfig)
{
    return MQTTSink::validateAndFormat(std::move(sinkConfig.config));
}

SinkRegistryReturnType SinkGeneratedRegistrar::RegisterMQTTSink(SinkRegistryArguments sinkRegistryArguments)
{
    return std::make_unique<MQTTSink>(sinkRegistryArguments.sinkDescriptor);
}

}
