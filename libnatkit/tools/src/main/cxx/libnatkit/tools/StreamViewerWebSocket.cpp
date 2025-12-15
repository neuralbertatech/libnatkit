#include "StreamViewerWebSocket.hpp"
#include <iostream>
#include <chrono>

using namespace drogon;

// Static member definition
std::shared_ptr<nat::kafka::BrokerManager> StreamViewerWebSocket::broker_manager_;

void StreamViewerWebSocket::handleNewConnection(const HttpRequestPtr& req,
                                                 const WebSocketConnectionPtr& conn)
{
    LOG_INFO << "StreamViewer WebSocket: New connection from " << conn->peerAddr().toIp();
    
    auto ctx = std::make_unique<StreamViewerClientContext>();
    
    {
        std::lock_guard<std::mutex> lock(clients_mutex_);
        clients_[conn] = std::move(ctx);
    }
    
    // Send initial status
    sendStatus(conn, clients_[conn].get());
}

void StreamViewerWebSocket::handleConnectionClosed(const WebSocketConnectionPtr& conn)
{
    LOG_INFO << "StreamViewer WebSocket: Connection closed";
    
    std::lock_guard<std::mutex> lock(clients_mutex_);
    auto it = clients_.find(conn);
    if (it != clients_.end()) {
        // Mark as inactive to stop streaming thread
        it->second->active = false;
        clients_.erase(it);
    }
}

void StreamViewerWebSocket::handleNewMessage(const WebSocketConnectionPtr& conn,
                                              std::string&& message,
                                              const WebSocketMessageType& type)
{
    // Ignore ping/pong and other control frames - they're handled by Drogon
    if (type == WebSocketMessageType::Ping || 
        type == WebSocketMessageType::Pong ||
        type == WebSocketMessageType::Close) {
        return;
    }
    
    if (type != WebSocketMessageType::Text) {
        sendError(conn, "Only text messages are supported");
        return;
    }

    try {
        auto json = nlohmann::json::parse(message);
        std::string action = json.value("action", "");

        StreamViewerClientContext* ctx = nullptr;
        {
            std::lock_guard<std::mutex> lock(clients_mutex_);
            auto it = clients_.find(conn);
            if (it != clients_.end()) {
                ctx = it->second.get();
            }
        }

        if (!ctx) {
            sendError(conn, "Client context not found");
            return;
        }

        if (action == "subscribe") {
            std::vector<uint64_t> stream_ids;
            if (json.contains("stream_ids") && json["stream_ids"].is_array()) {
                for (const auto& id : json["stream_ids"]) {
                    stream_ids.push_back(id.get<uint64_t>());
                }
            }
            handleSubscribe(conn, ctx, stream_ids);
        }
        else if (action == "unsubscribe") {
            std::vector<uint64_t> stream_ids;
            if (json.contains("stream_ids") && json["stream_ids"].is_array()) {
                for (const auto& id : json["stream_ids"]) {
                    stream_ids.push_back(id.get<uint64_t>());
                }
            }
            handleUnsubscribe(conn, ctx, stream_ids);
        }
        else if (action == "get_streams") {
            sendStreamList(conn);
        }
        else {
            sendError(conn, "Unknown action: " + action);
        }
    }
    catch (const std::exception& e) {
        sendError(conn, std::string("Failed to parse message: ") + e.what());
    }
}

void StreamViewerWebSocket::handleSubscribe(const WebSocketConnectionPtr& conn,
                                             StreamViewerClientContext* ctx,
                                             const std::vector<uint64_t>& stream_ids)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        // Get all available streams
        auto rawStreams = broker_manager_->getAllStreams();

        for (uint64_t stream_id : stream_ids) {
            if (ctx->subscribed_streams.count(stream_id) > 0) {
                continue; // Already subscribed
            }

            // Find the stream and create messengers for its DATA topics
            for (const auto& stream : rawStreams) {
                if (stream->getId() == stream_id) {
                    auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
                    for (auto& dataTopicUnique : dataTopics) {
                        std::shared_ptr<nat::core::BasicTopicInformation> dataTopic = std::move(dataTopicUnique);
                        ctx->messengers.push_back(broker_manager_->createMessenger(dataTopic));
                    }
                    ctx->subscribed_streams.insert(stream_id);
                    LOG_INFO << "StreamViewer: Subscribed to stream " << stream_id;
                    break;
                }
            }
        }

        // Start streaming thread if not already running
        if (!ctx->streaming_thread.joinable() && !ctx->subscribed_streams.empty()) {
            ctx->active = true;
            ctx->streaming_thread = std::thread(&StreamViewerWebSocket::streamingThreadFunc, this, conn, ctx);
        }
    } // Release lock before calling sendStatus

    sendStatus(conn, ctx);
}

void StreamViewerWebSocket::handleUnsubscribe(const WebSocketConnectionPtr& conn,
                                               StreamViewerClientContext* ctx,
                                               const std::vector<uint64_t>& stream_ids)
{
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);

        for (uint64_t stream_id : stream_ids) {
            ctx->subscribed_streams.erase(stream_id);
            
            // Remove messengers for this stream
            ctx->messengers.erase(
                std::remove_if(ctx->messengers.begin(), ctx->messengers.end(),
                    [stream_id](const std::unique_ptr<nat::core::TopicMessenger>& m) {
                        return m->getId() == stream_id;
                    }),
                ctx->messengers.end()
            );
            
            LOG_INFO << "StreamViewer: Unsubscribed from stream " << stream_id;
        }

        // Stop streaming thread if no subscriptions
        if (ctx->subscribed_streams.empty()) {
            ctx->active = false;
        }
    } // Release lock before calling sendStatus

    sendStatus(conn, ctx);
}

void StreamViewerWebSocket::sendStreamList(const WebSocketConnectionPtr& conn)
{
    if (!broker_manager_) {
        sendError(conn, "Broker manager not available");
        return;
    }

    nlohmann::json response;
    response["type"] = "stream_list";
    response["streams"] = nlohmann::json::object();

    auto rawStreams = broker_manager_->getAllStreams();
    for (const auto& stream : rawStreams) {
        uint64_t id = stream->getId();
        nlohmann::json stream_json;
        stream_json["topics"] = nlohmann::json::array();

        auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
        for (const auto& topic : dataTopics) {
            nlohmann::json topic_json;
            topic_json["schema_name"] = topic->schemaName;
            topic_json["type"] = "Data";
            topic_json["serialization_type"] = nat::core::toString(topic->serializationType);
            stream_json["topics"].push_back(topic_json);
        }

        auto metaTopics = stream->getTopicsByType(nat::core::StreamType::META);
        for (const auto& topic : metaTopics) {
            nlohmann::json topic_json;
            topic_json["schema_name"] = topic->schemaName;
            topic_json["type"] = "Meta";
            topic_json["serialization_type"] = nat::core::toString(topic->serializationType);
            stream_json["topics"].push_back(topic_json);
        }

        response["streams"][std::to_string(id)] = stream_json;
    }

    conn->send(response.dump());
}

void StreamViewerWebSocket::streamingThreadFunc(const WebSocketConnectionPtr& conn,
                                                 StreamViewerClientContext* ctx)
{
    LOG_INFO << "StreamViewer: Streaming thread started";

    while (ctx->active.load()) {
        // Check if connection is still valid
        if (!conn || !conn->connected()) {
            LOG_INFO << "StreamViewer: Connection lost, stopping streaming thread";
            ctx->active = false;
            break;
        }

        bool had_data = false;

        try {
            std::lock_guard<std::mutex> lock(ctx->mutex);
            
            for (auto& messenger : ctx->messengers) {
                if (!ctx->active.load()) break;  // Check again in case we should stop
                
                uint64_t stream_id = messenger->getId();
                std::string encoding_type = nat::core::toString(messenger->getSerializationType());

                // Try to get next message
                auto messageMaybe = messenger->tryGetNexMessage();
                if (messageMaybe.has_value()) {
                    had_data = true;
                    std::unique_ptr<nat::core::Schema> message = std::move(messageMaybe.value());

                    // Try to cast to NatImuBulkDataSchema first
                    nat::core::NatImuBulkDataSchema* bulkData = 
                        dynamic_cast<nat::core::NatImuBulkDataSchema*>(message.get());
                    
                    if (bulkData != nullptr) {
                        auto json = formatBulkDataAsJson(*bulkData, stream_id, encoding_type, 5000);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try single NatImuDataSchema
                    nat::core::NatImuDataSchema* imuData = 
                        dynamic_cast<nat::core::NatImuDataSchema*>(message.get());
                    
                    if (imuData != nullptr) {
                        auto json = formatImuDataAsJson(*imuData, stream_id, encoding_type, 50);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try NatMuseBulkDataSchema
                    nat::core::NatMuseBulkDataSchema* museBulkData = 
                        dynamic_cast<nat::core::NatMuseBulkDataSchema*>(message.get());
                    
                    if (museBulkData != nullptr) {
                        auto json = formatMuseBulkDataAsJson(*museBulkData, stream_id, encoding_type, 34900);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }

                    // Try single NatMuseDataSchema
                    nat::core::NatMuseDataSchema* museData = 
                        dynamic_cast<nat::core::NatMuseDataSchema*>(message.get());
                    
                    if (museData != nullptr) {
                        auto json = formatMuseDataAsJson(*museData, stream_id, encoding_type, 349);
                        if (conn && conn->connected()) {
                            conn->send(json.dump());
                        }
                        continue;
                    }
                }
            }
        } catch (const std::exception& e) {
            LOG_ERROR << "StreamViewer: Error in streaming thread: " << e.what();
        }

        // Small sleep to avoid busy-waiting
        if (!had_data) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
    }

    LOG_INFO << "StreamViewer: Streaming thread stopped";
}

nlohmann::json StreamViewerWebSocket::formatImuDataAsJson(const nat::core::NatImuDataSchema& data,
                                                           uint64_t stream_id,
                                                           const std::string& encoding_type,
                                                           size_t encoding_size)
{
    const float* values = data.getData();

    nlohmann::json json;
    json["type"] = "imu_data";
    json["stream_id"] = stream_id;
    json["timestamp"] = static_cast<uint64_t>(data.getTime());
    
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;

    json["data"]["accel"]["x"] = values[0];
    json["data"]["accel"]["y"] = values[1];
    json["data"]["accel"]["z"] = values[2];

    json["data"]["gyro"]["x"] = values[3];
    json["data"]["gyro"]["y"] = values[4];
    json["data"]["gyro"]["z"] = values[5];

    json["data"]["quat"]["real"] = values[6];
    json["data"]["quat"]["i"] = values[7];
    json["data"]["quat"]["j"] = values[8];
    json["data"]["quat"]["k"] = values[9];

    json["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getAccelerationAccuracy());
    json["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getGyroscopeAccuracy());
    json["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
        data.getRotationAccuracy());

    json["has_data"]["accelerometer"] = data.wasDataSetForAcceleration();
    json["has_data"]["gyroscope"] = data.wasDataSetForGryoscope();
    json["has_data"]["rotation"] = data.wasDataSetForRotation();

    return json;
}

nlohmann::json StreamViewerWebSocket::formatBulkDataAsJson(const nat::core::NatImuBulkDataSchema& bulk,
                                                            uint64_t stream_id,
                                                            const std::string& encoding_type,
                                                            size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "imu_bulk_data";
    json["stream_id"] = stream_id;
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["samples"] = nlohmann::json::array();

    auto records = bulk.createImuRecords();
    if (records) {
        for (const auto& record : *records) {
            const float* values = record.getData();
            
            nlohmann::json sample;
            sample["timestamp"] = static_cast<uint64_t>(record.getTime());
            
            sample["data"]["accel"]["x"] = values[0];
            sample["data"]["accel"]["y"] = values[1];
            sample["data"]["accel"]["z"] = values[2];

            sample["data"]["gyro"]["x"] = values[3];
            sample["data"]["gyro"]["y"] = values[4];
            sample["data"]["gyro"]["z"] = values[5];

            sample["data"]["quat"]["real"] = values[6];
            sample["data"]["quat"]["i"] = values[7];
            sample["data"]["quat"]["j"] = values[8];
            sample["data"]["quat"]["k"] = values[9];

            sample["accuracies"]["accelerometer"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getAccelerationAccuracy());
            sample["accuracies"]["gyroscope"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getGyroscopeAccuracy());
            sample["accuracies"]["rotation"] = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                record.getRotationAccuracy());

            sample["has_data"]["accelerometer"] = record.wasDataSetForAcceleration();
            sample["has_data"]["gyroscope"] = record.wasDataSetForGryoscope();
            sample["has_data"]["rotation"] = record.wasDataSetForRotation();

            json["samples"].push_back(sample);
        }
    }

    return json;
}

nlohmann::json StreamViewerWebSocket::formatMuseDataAsJson(const nat::core::NatMuseDataSchema& data,
                                                            uint64_t stream_id,
                                                            const std::string& encoding_type,
                                                            size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "muse_data";
    json["stream_id"] = stream_id;
    json["timestamp"] = data.getTime();
    json["eeg_sequence"] = data.getEegSequence();
    json["motion_sequence"] = data.getMotionSequence();

    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;

    // EEG data (4 channels × 12 samples)
    const float* tp9 = data.getTp9();
    const float* af7 = data.getAf7();
    const float* af8 = data.getAf8();
    const float* tp10 = data.getTp10();

    json["eeg"]["tp9"] = nlohmann::json::array();
    json["eeg"]["af7"] = nlohmann::json::array();
    json["eeg"]["af8"] = nlohmann::json::array();
    json["eeg"]["tp10"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::EEG_SAMPLES_PER_PACKET; ++i) {
        json["eeg"]["tp9"].push_back(tp9[i]);
        json["eeg"]["af7"].push_back(af7[i]);
        json["eeg"]["af8"].push_back(af8[i]);
        json["eeg"]["tp10"].push_back(tp10[i]);
    }

    // Motion data (3 samples each)
    const float (*accel)[3] = data.getAccel();
    const float (*gyro)[3] = data.getGyro();

    json["accel"] = nlohmann::json::array();
    json["gyro"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::MOTION_SAMPLES; ++i) {
        json["accel"].push_back({{"x", accel[i][0]}, {"y", accel[i][1]}, {"z", accel[i][2]}});
        json["gyro"].push_back({{"x", gyro[i][0]}, {"y", gyro[i][1]}, {"z", gyro[i][2]}});
    }

    // PPG data (3 channels × 6 samples)
    const float* ppg0 = data.getPpg0();
    const float* ppg1 = data.getPpg1();
    const float* ppg2 = data.getPpg2();

    json["ppg"]["ppg0"] = nlohmann::json::array();
    json["ppg"]["ppg1"] = nlohmann::json::array();
    json["ppg"]["ppg2"] = nlohmann::json::array();

    for (int i = 0; i < nat::core::NatMuseDataSchema::PPG_SAMPLES; ++i) {
        json["ppg"]["ppg0"].push_back(ppg0[i]);
        json["ppg"]["ppg1"].push_back(ppg1[i]);
        json["ppg"]["ppg2"].push_back(ppg2[i]);
    }

    // has_data flags
    json["has_data"]["eeg"] = data.hasEegData();
    json["has_data"]["accel"] = data.hasAccelData();
    json["has_data"]["gyro"] = data.hasGyroData();
    json["has_data"]["ppg"] = data.hasPpgData();

    return json;
}

nlohmann::json StreamViewerWebSocket::formatMuseBulkDataAsJson(const nat::core::NatMuseBulkDataSchema& bulk,
                                                                uint64_t stream_id,
                                                                const std::string& encoding_type,
                                                                size_t encoding_size)
{
    nlohmann::json json;
    json["type"] = "muse_bulk_data";
    json["stream_id"] = stream_id;
    json["encoding"]["type"] = encoding_type;
    json["encoding"]["size"] = encoding_size;
    json["samples"] = nlohmann::json::array();

    auto records = bulk.createMuseRecords();
    if (records) {
        for (const auto& record : *records) {
            nlohmann::json sample;
            sample["timestamp"] = record.getTime();
            sample["eeg_sequence"] = record.getEegSequence();
            sample["motion_sequence"] = record.getMotionSequence();

            // EEG data
            const float* tp9 = record.getTp9();
            const float* af7 = record.getAf7();
            const float* af8 = record.getAf8();
            const float* tp10 = record.getTp10();

            sample["eeg"]["tp9"] = nlohmann::json::array();
            sample["eeg"]["af7"] = nlohmann::json::array();
            sample["eeg"]["af8"] = nlohmann::json::array();
            sample["eeg"]["tp10"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::EEG_SAMPLES_PER_PACKET; ++i) {
                sample["eeg"]["tp9"].push_back(tp9[i]);
                sample["eeg"]["af7"].push_back(af7[i]);
                sample["eeg"]["af8"].push_back(af8[i]);
                sample["eeg"]["tp10"].push_back(tp10[i]);
            }

            // Motion data
            const float (*accel)[3] = record.getAccel();
            const float (*gyro)[3] = record.getGyro();

            sample["accel"] = nlohmann::json::array();
            sample["gyro"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::MOTION_SAMPLES; ++i) {
                sample["accel"].push_back({{"x", accel[i][0]}, {"y", accel[i][1]}, {"z", accel[i][2]}});
                sample["gyro"].push_back({{"x", gyro[i][0]}, {"y", gyro[i][1]}, {"z", gyro[i][2]}});
            }

            // PPG data
            const float* ppg0 = record.getPpg0();
            const float* ppg1 = record.getPpg1();
            const float* ppg2 = record.getPpg2();

            sample["ppg"]["ppg0"] = nlohmann::json::array();
            sample["ppg"]["ppg1"] = nlohmann::json::array();
            sample["ppg"]["ppg2"] = nlohmann::json::array();

            for (int i = 0; i < nat::core::NatMuseDataSchema::PPG_SAMPLES; ++i) {
                sample["ppg"]["ppg0"].push_back(ppg0[i]);
                sample["ppg"]["ppg1"].push_back(ppg1[i]);
                sample["ppg"]["ppg2"].push_back(ppg2[i]);
            }

            // has_data flags
            sample["has_data"]["eeg"] = record.hasEegData();
            sample["has_data"]["accel"] = record.hasAccelData();
            sample["has_data"]["gyro"] = record.hasGyroData();
            sample["has_data"]["ppg"] = record.hasPpgData();

            json["samples"].push_back(sample);
        }
    }

    return json;
}

void StreamViewerWebSocket::sendError(const WebSocketConnectionPtr& conn, const std::string& message)
{
    nlohmann::json json;
    json["type"] = "error";
    json["message"] = message;
    conn->send(json.dump());
}

void StreamViewerWebSocket::sendStatus(const WebSocketConnectionPtr& conn, StreamViewerClientContext* ctx)
{
    nlohmann::json json;
    json["type"] = "status";
    json["connected"] = true;
    json["subscribed_streams"] = nlohmann::json::array();
    
    {
        std::lock_guard<std::mutex> lock(ctx->mutex);
        for (uint64_t id : ctx->subscribed_streams) {
            json["subscribed_streams"].push_back(id);
        }
    }

    conn->send(json.dump());
}
