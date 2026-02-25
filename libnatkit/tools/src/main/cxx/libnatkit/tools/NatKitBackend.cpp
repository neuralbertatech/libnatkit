#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <set>
#include <stdlib.h>
#include <thread>
#include <random>
#include <sstream>
#include <iomanip>

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
#include <libnatkit/util/Strings.hpp>
#include <libnatkit/util/Vectors.hpp>

#include <nlohmann/json.hpp>

#include <drogon/drogon.h>
#include <drogon/HttpFilter.h>
#include <drogon/WebSocketController.h>
#include <iostream>

#include "StreamViewerWebSocket.hpp"

using namespace drogon;
using namespace std::chrono_literals;

// Simple UUID generator for session IDs
std::string generate_uuid() {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<> dis(0, 15);
    static const char* hex = "0123456789abcdef";

    std::stringstream ss;
    for (int i = 0; i < 32; ++i) {
        if (i == 8 || i == 12 || i == 16 || i == 20) {
            ss << '-';
        }
        ss << hex[dis(gen)];
    }
    return ss.str();
}

// Structure to hold a marker event
struct Marker {
    std::string marker_type;  // task_start, task_end, session_start, session_end
    std::string task_id;      // e.g., "drink_cup"
    uint64_t timestamp;       // milliseconds since epoch
    std::string session_id;
};

// Structure to hold IMU data sample
struct ImuSample {
    uint64_t timestamp;
    uint64_t stream_id;
    std::string sensor_position;
    float quat_i, quat_j, quat_k, quat_real;
    float accel_x, accel_y, accel_z;
    float gyro_x, gyro_y, gyro_z;
    float gravity_x, gravity_y, gravity_z;
};

// Recording session state
struct RecordingSession {
    std::string session_id;
    std::atomic<bool> is_recording{false};
    uint64_t start_time = 0;
    std::vector<Marker> markers;
    std::vector<ImuSample> samples;
    std::map<uint64_t, std::string> stream_position_mapping;  // stream_id -> sensor_position
    std::mutex mutex;
    
    // Delete copy operations since mutex is not copyable
    RecordingSession() = default;
    RecordingSession(const RecordingSession&) = delete;
    RecordingSession& operator=(const RecordingSession&) = delete;
    RecordingSession(RecordingSession&&) = delete;
    RecordingSession& operator=(RecordingSession&&) = delete;
};

// Structure to hold all three IMU accuracy values
struct ImuAccuracies {
    int accelerometer = 0;
    int gyroscope = 0;
    int rotation = 0;
};

class Config {
    std::vector<std::string> ids{};
    std::map<std::string, std::string> idNames{};

    void populateDataFromJsonFile() {
        std::ifstream jsonFile("config.json");
        nlohmann::json json = nlohmann::json::parse(jsonFile);
        for (auto& [index, listEntry] : json["imu"].items()) {
            for (auto& [imuId, imuFields] : listEntry.items()) {
                ids.push_back(imuId);
                for (auto& [imuFieldName, imuFieldValue] : imuFields.items()) {
                    if (imuFieldName == "name") {
                        idNames.insert({ imuId, imuFieldValue.get<std::string>() });
                    }
                }
            }
        }
        // std::vector<ImuConfig> imus = json["imu"].template get<std::vector<ImuConfig>>();
        // for (const auto& imu : imus) {
        //     ids.insert(imu.id);
        //     idNames.insert({ imu.id, imu.name });
        // }
    }

public:
    Config() {
        //populateDataFromJsonFile();
    }

    std::optional<std::string> tryGetName(std::string id) {
        if (auto it = idNames.find(id); it != idNames.end()) {
            return it->second;
        }
        else {
            return {};
        }
    }
};

//class ImuApplication {
//    enum class ImuApplicationStage {
//        SelectImus = 1,
//        Calibration,
//    };
//
//    Config config;
//    std::unique_ptr<nat::kafka::BrokerManager> manager;
//    ImuApplicationStage currentStage;
//    std::optional<std::vector<std::unique_ptr<nat::core::TopicMessenger>>> imuStreams;
//
//    static std::vector<std::unique_ptr<nat::core::TopicMessenger>> getImuStreams(const std::unique_ptr<nat::kafka::BrokerManager>& manager, std::shared_ptr<ImuTuiWindow> window, std::shared_ptr<KeyListener> keyListener) {
//        const std::vector<std::unique_ptr<nat::core::RawStream>> streams = promptUserToChooseStreams(manager, window, keyListener);
//        std::vector<std::unique_ptr<nat::core::TopicMessenger>> messengers{};
//        for (const auto& stream : streams) {
//            auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
//            if (dataTopics.size() > 0) {
//                std::shared_ptr<nat::core::BasicTopicInformation> dataTopic = std::move(stream->getTopicsByType(nat::core::StreamType::DATA)[0]);
//                messengers.emplace_back(manager->createMessenger(dataTopic));
//            }
//        }
//
//        return messengers;
//    }
//
//    std::string getNameForImu(std::string id) {
//        std::optional<std::string> nameMaybe = config.tryGetName(id);
//        if (nameMaybe.has_value())
//            return nameMaybe.value();
//        else
//            return "?";
//    }
//
//public:
//    ImuApplication(std::unique_ptr<nat::kafka::BrokerManager> manager, Config config)
//        : manager(std::move(manager)), currentStage(ImuApplicationStage::SelectImus), config(config) {}
//
//    void start() {
//        while (true) {
//            switch (currentStage) {
//            case ImuApplicationStage::SelectImus:
//                if (!imuStreams.has_value()) {
//                    imuStreams = getImuStreams(manager);
//                }
//                else {
//                    currentStage = ImuApplicationStage::Calibration;
//                }
//                break;
//
//            case ImuApplicationStage::Calibration:
//                std::vector<std::string> imuAccuracies{};
//                for (auto& stream : imuStreams.value()) {
//                    stream->clearAllMessages();
//                    std::shared_ptr<nat::core::Schema> message{ nullptr };
//                    const auto initailWaitUntil = std::chrono::system_clock::now() + 1s;
//                    while (std::chrono::system_clock::now() < initailWaitUntil) {
//                        auto messageMaybe = stream->tryGetNexMessage();
//                        if (messageMaybe.has_value()) {
//                            message = std::move(messageMaybe.value());
//                            break;
//                        }
//                    }
//                    if (message == nullptr) {
//                        window->addOutput("No messages recieved from " + std::to_string(stream->getId()) + ". Is it connected?");
//                        continue;
//                    }
//                    else {
//                        nat::core::Optional<std::shared_ptr<nat::core::NatImuDataSchema>> convertedMessageMaybe = nat::core::NatImuDataSchema::tryCreateFromSchema(nat::core::Optional<const std::shared_ptr<nat::core::Schema>>(message));
//                        if (!convertedMessageMaybe.has_value()) {
//                            window->addOutput(std::to_string(stream->getId()) + ": Was expecting a NatImuDataSchema object, but found a " + message->getName() + " object instead. Error");
//                            continue;
//                        }
//                        else {
//                            std::string id = std::to_string(stream->getId());
//                            std::string name = getNameForImu(id);
//                            imuAccuracies.push_back(name + ":");
//                            imuAccuracies.push_back("    Id = " + id);
//                            imuAccuracies.push_back("    Accuracy = " + std::to_string(nat::core::NatImuDataSchema::convertSensorAccuracyToInt(convertedMessageMaybe.value()->getAccelerationAccuracy())) + ", "
//                                + std::to_string(nat::core::NatImuDataSchema::convertSensorAccuracyToInt(convertedMessageMaybe.value()->getGyroscopeAccuracy())) + ", "
//                                + std::to_string(nat::core::NatImuDataSchema::convertSensorAccuracyToInt(convertedMessageMaybe.value()->getRotationAccuracy())));
//                            imuAccuracies.push_back("");
//                        }
//                    }
//                }
//                window->setSidePannelContent(imuAccuracies);
//                break;
//            }
//        }
//    }
//};

class CorsFilter : public drogon::HttpFilter<CorsFilter, false>
{
public:
    virtual void doFilter(const drogon::HttpRequestPtr& req,
        drogon::FilterCallback&& fcb,
        drogon::FilterChainCallback&& fccb) override
    {
        std::cout << "Do filter hit\n" << req->getBody() << '\n';
        // Check if it's a preflight request (OPTIONS method)
        if (req->method() == drogon::HttpMethod::Options)
        {
            auto resp = drogon::HttpResponse::newHttpResponse();
            // Add headers to the response to tell the browser it's safe
            // Allow any localhost port for development
            resp->addHeader("Access-Control-Allow-Origin", req->getHeader("Origin"));
            resp->addHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
            resp->addHeader("Access-Control-Allow-Headers", "Content-Type");
            resp->addHeader("Access-Control-Max-Age", "3600");

            // Return this response immediately and stop the request chain
            fcb(resp);
            return;
        }

        // If it's not a preflight request, continue to the endpoint handler
        fccb();
    }
};

// Define a controller class to handle the API requests.
// Inheriting from HttpController makes it easy to register routes.
class ApiController : public drogon::HttpController<ApiController, false> {
    Config config;
    std::shared_ptr<nat::kafka::BrokerManager> manager;
    std::optional<std::vector<std::unique_ptr<nat::core::TopicMessenger>>> imuStreams;
    std::map<uint64_t, ImuAccuracies> lastKnownAccuraciesByStreamId;
    std::mutex lastKnownAccuraciesLock;
    std::unique_ptr<RecordingSession> recording_session;
    std::thread recording_thread;
    std::atomic<bool> stop_recording_thread{false};

    void set_imu_streams(const std::set<uint64_t>& stream_ids) {
        auto rawStreams = manager->getAllStreams();
        std::vector<std::unique_ptr<nat::core::TopicMessenger>> new_imu_streams{};
        for (const auto& stream : rawStreams) {
            auto id = stream->getId();
            if (stream_ids.find(id) != stream_ids.end()) {
                auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
                for (auto& dataTopicUnique : dataTopics) {
                    std::shared_ptr<nat::core::BasicTopicInformation> dataTopic = std::move(dataTopicUnique);
                    new_imu_streams.push_back(manager->createMessenger(dataTopic));
                }
            }
        }

        if (new_imu_streams.size() > 0) {
            // Keep cache entries only for selected stream IDs
            {
                const std::lock_guard<std::mutex> guard(lastKnownAccuraciesLock);
                for (auto it = lastKnownAccuraciesByStreamId.begin(); it != lastKnownAccuraciesByStreamId.end();) {
                    if (stream_ids.find(it->first) == stream_ids.end()) {
                        it = lastKnownAccuraciesByStreamId.erase(it);
                    } else {
                        ++it;
                    }
                }
            }
            imuStreams = std::move(new_imu_streams);
        }
    }

public:
    // Macro to map routes to member functions.
    // This is how Drogon connects a URL path to your code.
    METHOD_LIST_BEGIN
    METHOD_ADD(ApiController::heartbeat, "/api/heartbeat", Get);
    METHOD_ADD(ApiController::is_connected_to_broker, "/api/is_connected_to_broker", Get);
    METHOD_ADD(ApiController::get_accuracies, "/api/get_accuracies", Get);
    METHOD_ADD(ApiController::get_all_streams, "/api/get_all_streams", Get);
    METHOD_ADD(ApiController::set_streams, "/api/set_streams", Post);
    METHOD_ADD(ApiController::start_calibration, "/api/start_calibration", Post);
    METHOD_ADD(ApiController::start_recording, "/api/start_recording", Post);
    METHOD_ADD(ApiController::stop_recording, "/api/stop_recording", Post);
    METHOD_ADD(ApiController::insert_marker, "/api/insert_marker", Post);
    METHOD_ADD(ApiController::get_session_data, "/api/get_session_data", Get);
    METHOD_LIST_END

    ApiController(std::shared_ptr<nat::kafka::BrokerManager> manager, Config config)
        : manager(std::move(manager)), config(config), recording_session(std::make_unique<RecordingSession>()) {}

    ~ApiController() {
        // Stop recording thread if running
        stop_recording_thread = true;
        if (recording_thread.joinable()) {
            recording_thread.join();
        }
    }

    // Handler for GET /api/heartbeat
    // A simple health check endpoint.
    void heartbeat(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        Json::Value resp_json;
        resp_json["connected"] = true;
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for GET /api/is_connected_to_broker
    // Stubs a check for a message broker connection (e.g., RabbitMQ, Kafka).
    void is_connected_to_broker(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        // TODO: Implement actual logic to check broker connection status.
        // For now, we'll just return a placeholder.
        Json::Value resp_json;
        if (manager->isConnected()) {
            resp_json["connected"] = true;
            auto resp = HttpResponse::newHttpJsonResponse(resp_json);
            resp->setStatusCode(drogon::HttpStatusCode::k200OK);
            resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
            callback(resp);
        }
        else {
            resp_json["connected"] = false;
            auto resp = HttpResponse::newHttpJsonResponse(resp_json);
            resp->setStatusCode(HttpStatusCode::k404NotFound);
            callback(resp);
        }
    }

    // Helper to extract all accuracies from a single NatImuDataSchema
    ImuAccuracies getAccuraciesFromImuData(nat::core::NatImuDataSchema* imuData) {
        ImuAccuracies result;
        if (imuData == nullptr) return result;
        result.accelerometer = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
            imuData->getAccelerationAccuracy());
        result.gyroscope = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
            imuData->getGyroscopeAccuracy());
        result.rotation = nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
            imuData->getRotationAccuracy());
        return result;
    }

    // Handler for GET /api/get_accuracies
    // Returns calibration accuracy for each selected stream.
    // Values: 0=Unreliable, 1=Low, 2=Medium, 3=High
    void get_accuracies(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        Json::Value resp_json;
        Json::Value accuracies_json;
        
        // Read actual accuracy from each IMU stream
        if (imuStreams.has_value()) {
            for (auto& stream : imuStreams.value()) {
                uint64_t id = stream->getId();
                ImuAccuracies accuracies; // Default fallback: all Unreliable (0)
                {
                    const std::lock_guard<std::mutex> guard(lastKnownAccuraciesLock);
                    auto it = lastKnownAccuraciesByStreamId.find(id);
                    if (it != lastKnownAccuraciesByStreamId.end()) {
                        accuracies = it->second;
                    }
                }
                
                // Try to get the latest message from the stream
                nat::core::Optional<std::unique_ptr<nat::core::Schema>> messageMaybe = stream->tryGetNexMessage();
                if (messageMaybe.has_value()) {
                    std::unique_ptr<nat::core::Schema> message = std::move(messageMaybe.value());
                    
                    // Try to cast to NatImuDataSchema first
                    nat::core::NatImuDataSchema* imuData = dynamic_cast<nat::core::NatImuDataSchema*>(message.get());
                    if (imuData != nullptr) {
                        accuracies = getAccuraciesFromImuData(imuData);
                    } else {
                        // Try to cast to NatImuBulkDataSchema (bulk data)
                        nat::core::NatImuBulkDataSchema* bulkData = dynamic_cast<nat::core::NatImuBulkDataSchema*>(message.get());
                        if (bulkData != nullptr) {
                            // Get the records and use the last one for accuracy
                            std::unique_ptr<std::vector<nat::core::NatImuDataSchema>> records = bulkData->createImuRecords();
                            if (records && !records->empty()) {
                                // Use the last record's accuracy (most recent)
                                accuracies = getAccuraciesFromImuData(&records->back());
                            }
                        }
                    }
                    {
                        const std::lock_guard<std::mutex> guard(lastKnownAccuraciesLock);
                        lastKnownAccuraciesByStreamId[id] = accuracies;
                    }
                }
                
                // Return all three accuracy values for each stream
                Json::Value stream_accuracies;
                stream_accuracies["accelerometer"] = accuracies.accelerometer;
                stream_accuracies["gyroscope"] = accuracies.gyroscope;
                stream_accuracies["rotation"] = accuracies.rotation;
                accuracies_json[std::to_string(id)] = stream_accuracies;
            }
        }
        
        resp_json["accuracies"] = accuracies_json;
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for GET /api/get_all_streams
    // Stubs a request to list all available data streams.
    void get_all_streams(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        // Check if broker is connected before trying to get streams
        if (!manager || !manager->isConnected()) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "Broker not connected";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k503ServiceUnavailable);
            resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
            callback(resp);
            return;
        }

        try {
            auto rawStreams = manager->getAllStreams();
            Json::Value json;
            for (const auto& stream : rawStreams) {
                auto id = stream->getId();
                auto dataTopics = stream->getTopicsByType(nat::core::StreamType::DATA);
                auto metaTopics = stream->getTopicsByType(nat::core::StreamType::META);
                Json::Value topics;
                for (const auto& dataTopic : dataTopics) {
                    Json::Value topic;
                    topic["schema_name"] = dataTopic->schemaName;
                    topic["type"] = toString(dataTopic->type);
                    topic["serialization_type"] = toString(dataTopic->serializationType);
                    topics.append(topic);
                }
                for (const auto& metaTopic : metaTopics) {
                    Json::Value topic;
                    topic["schema_name"] = metaTopic->schemaName;
                    topic["type"] = toString(metaTopic->type);
                    topic["serialization_type"] = toString(metaTopic->serializationType);
                    topics.append(topic);
                }
                json[std::to_string(id)]["topics"] = topics;
            }

            auto resp = HttpResponse::newHttpJsonResponse(json);
            resp->setStatusCode(drogon::HttpStatusCode::k200OK);
            resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
            callback(resp);
        } catch (const std::exception& e) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = std::string("Failed to get streams: ") + e.what();
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k500InternalServerError);
            resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
            callback(resp);
        }
    }

    // Handler for POST /api/set_streams
    // Stubs a request to configure which streams are active.
    void set_streams(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        bool failed = false;
        // Asynchronously parse the JSON body of the request.
        if (req->getContentType() != drogon::ContentType::CT_APPLICATION_JSON) {
            std::cerr << "ERRROR: Was expecting JSON content\n";
            failed = true;
        }
        else {
            auto json = req->getJsonObject();
            if (!json) {
                std::cerr << "ERRROR: Was unable to parse JSON from the payload\n";
                failed = true;
            }
            else if (!json->isMember("stream_ids")) {
                std::cerr << "ERRROR: \"stream_ids\" was not present in the payload\n";
                failed = true;
            }
            else {
                std::vector<uint64_t> ids{};
                for (const auto& jsonId : (*json)["stream_ids"]) {
                    uint64_t id = jsonId.asUInt64();
                    std::cout << "Watching stream " << id << '\n';
                    ids.push_back(id);
                }
                set_imu_streams(std::set<uint64_t>{ids.begin(), ids.end()});
                Json::Value resp_json;
                resp_json["status"] = "success";
                resp_json["message"] = "Stream configuration received.";
                auto resp = HttpResponse::newHttpJsonResponse(resp_json);
                resp->setStatusCode(drogon::HttpStatusCode::k200OK);
                resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
                callback(resp);
                return;
            }
        }

        if (failed) {
            std::cout << "There was an error\n";
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "Invalid JSON format.";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k400BadRequest);
            callback(resp);
        }
    }

    // Handler for POST /api/start_calibration
    // Stubs a request to begin a calibration process.
    void start_calibration(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        // TODO: Implement logic to trigger a long-running calibration task.
        // This could involve spawning a new thread or adding a job to a queue.
        LOG_INFO << "Calibration process initiated by API request.";
        
        Json::Value resp_json;
        resp_json["status"] = "success";
        resp_json["message"] = "Calibration process started.";
        resp_json["job_id"] = "calib_12345"; // Example job ID
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Background thread function to collect IMU samples during recording
    void recording_thread_func() {
        LOG_INFO << "Recording thread started";
        while (!stop_recording_thread.load()) {
            if (!recording_session->is_recording.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }

            // Read from all IMU streams
            if (imuStreams.has_value()) {
                for (auto& stream : imuStreams.value()) {
                    uint64_t stream_id = stream->getId();
                    
                    // Get sensor position for this stream
                    std::string sensor_position = "Unknown";
                    {
                        std::lock_guard<std::mutex> lock(recording_session->mutex);
                        auto it = recording_session->stream_position_mapping.find(stream_id);
                        if (it != recording_session->stream_position_mapping.end()) {
                            sensor_position = it->second;
                        }
                    }

                    // Try to get messages from stream
                    nat::core::Optional<std::unique_ptr<nat::core::Schema>> messageMaybe = stream->tryGetNexMessage();
                    if (messageMaybe.has_value()) {
                        std::unique_ptr<nat::core::Schema> message = std::move(messageMaybe.value());
                        
                        // Try to cast to NatImuBulkDataSchema (bulk data)
                        nat::core::NatImuBulkDataSchema* bulkData = dynamic_cast<nat::core::NatImuBulkDataSchema*>(message.get());
                        if (bulkData != nullptr) {
                            std::unique_ptr<std::vector<nat::core::NatImuDataSchema>> records = bulkData->createImuRecords();
                            if (records) {
                                std::lock_guard<std::mutex> lock(recording_session->mutex);
                                for (const auto& record : *records) {
                                    const float* data = record.getData();
                                    ImuSample sample;
                                    sample.timestamp = static_cast<uint64_t>(record.getTime());
                                    sample.stream_id = stream_id;
                                    sample.sensor_position = sensor_position;
                                    // data[0-2]: accelerometer x, y, z
                                    sample.accel_x = data[0];
                                    sample.accel_y = data[1];
                                    sample.accel_z = data[2];
                                    // data[3-5]: gyroscope x, y, z
                                    sample.gyro_x = data[3];
                                    sample.gyro_y = data[4];
                                    sample.gyro_z = data[5];
                                    // data[6-9]: rotation quaternion (real, i, j, k)
                                    sample.quat_real = data[6];
                                    sample.quat_i = data[7];
                                    sample.quat_j = data[8];
                                    sample.quat_k = data[9];
                                    // No gravity data in current format
                                    sample.gravity_x = 0.0f;
                                    sample.gravity_y = 0.0f;
                                    sample.gravity_z = 0.0f;
                                    recording_session->samples.push_back(sample);
                                }
                            }
                        } else {
                            // Try single NatImuDataSchema
                            nat::core::NatImuDataSchema* imuData = dynamic_cast<nat::core::NatImuDataSchema*>(message.get());
                            if (imuData != nullptr) {
                                const float* data = imuData->getData();
                                std::lock_guard<std::mutex> lock(recording_session->mutex);
                                ImuSample sample;
                                sample.timestamp = static_cast<uint64_t>(imuData->getTime());
                                sample.stream_id = stream_id;
                                sample.sensor_position = sensor_position;
                                sample.accel_x = data[0];
                                sample.accel_y = data[1];
                                sample.accel_z = data[2];
                                sample.gyro_x = data[3];
                                sample.gyro_y = data[4];
                                sample.gyro_z = data[5];
                                sample.quat_real = data[6];
                                sample.quat_i = data[7];
                                sample.quat_j = data[8];
                                sample.quat_k = data[9];
                                sample.gravity_x = 0.0f;
                                sample.gravity_y = 0.0f;
                                sample.gravity_z = 0.0f;
                                recording_session->samples.push_back(sample);
                            }
                        }
                    }
                }
            }
            
            // Small sleep to avoid busy-waiting
            std::this_thread::sleep_for(std::chrono::milliseconds(1));
        }
        LOG_INFO << "Recording thread stopped";
    }

    // Handler for POST /api/start_recording
    // Begins a new recording session for ADL experiment
    void start_recording(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        std::lock_guard<std::mutex> lock(recording_session->mutex);
        
        if (recording_session->is_recording) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "Recording is already in progress.";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k400BadRequest);
            callback(resp);
            return;
        }

        // Parse optional stream_position_mapping from request
        auto json = req->getJsonObject();
        if (json && json->isMember("stream_position_mapping")) {
            recording_session->stream_position_mapping.clear();
            for (const auto& key : (*json)["stream_position_mapping"].getMemberNames()) {
                uint64_t stream_id = std::stoull(key);
                std::string position = (*json)["stream_position_mapping"][key].asString();
                recording_session->stream_position_mapping[stream_id] = position;
            }
        }

        // Initialize new session
        recording_session->session_id = generate_uuid();
        recording_session->is_recording = true;
        recording_session->start_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
        recording_session->markers.clear();
        recording_session->samples.clear();

        // Start recording thread if not already running
        if (!recording_thread.joinable()) {
            stop_recording_thread = false;
            recording_thread = std::thread(&ApiController::recording_thread_func, this);
        }

        LOG_INFO << "Recording session started: " << recording_session->session_id;

        Json::Value resp_json;
        resp_json["status"] = "success";
        resp_json["message"] = "Recording started.";
        resp_json["session_id"] = recording_session->session_id;
        resp_json["start_time"] = Json::Value::UInt64(recording_session->start_time);
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for POST /api/stop_recording
    // Ends the current recording session
    void stop_recording(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        std::lock_guard<std::mutex> lock(recording_session->mutex);
        
        if (!recording_session->is_recording) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "No recording in progress.";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k400BadRequest);
            callback(resp);
            return;
        }

        recording_session->is_recording = false;
        uint64_t end_time = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();

        LOG_INFO << "Recording session stopped: " << recording_session->session_id;

        Json::Value resp_json;
        resp_json["status"] = "success";
        resp_json["message"] = "Recording stopped.";
        resp_json["session_id"] = recording_session->session_id;
        resp_json["start_time"] = Json::Value::UInt64(recording_session->start_time);
        resp_json["end_time"] = Json::Value::UInt64(end_time);
        resp_json["marker_count"] = Json::Value::UInt(recording_session->markers.size());
        resp_json["sample_count"] = Json::Value::UInt(recording_session->samples.size());
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for POST /api/insert_marker
    // Inserts an event marker into the recording session
    void insert_marker(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        if (req->getContentType() != drogon::ContentType::CT_APPLICATION_JSON) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "Expected JSON content.";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k400BadRequest);
            callback(resp);
            return;
        }

        auto json = req->getJsonObject();
        if (!json || !json->isMember("marker_type")) {
            Json::Value err_json;
            err_json["status"] = "error";
            err_json["message"] = "Missing required field: marker_type";
            auto resp = HttpResponse::newHttpJsonResponse(err_json);
            resp->setStatusCode(k400BadRequest);
            callback(resp);
            return;
        }

        std::lock_guard<std::mutex> lock(recording_session->mutex);

        Marker marker;
        marker.marker_type = (*json)["marker_type"].asString();
        marker.task_id = json->isMember("task_id") ? (*json)["task_id"].asString() : "";
        marker.timestamp = json->isMember("timestamp") 
            ? (*json)["timestamp"].asUInt64()
            : std::chrono::duration_cast<std::chrono::milliseconds>(
                std::chrono::system_clock::now().time_since_epoch()).count();
        marker.session_id = recording_session->session_id;

        recording_session->markers.push_back(marker);

        LOG_INFO << "Marker inserted: " << marker.marker_type << " " << marker.task_id;

        Json::Value resp_json;
        resp_json["status"] = "success";
        resp_json["message"] = "Marker inserted.";
        resp_json["marker_type"] = marker.marker_type;
        resp_json["task_id"] = marker.task_id;
        resp_json["timestamp"] = Json::Value::UInt64(marker.timestamp);
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for GET /api/get_session_data
    // Returns all recorded data (IMU samples + markers) for CSV export
    void get_session_data(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        std::lock_guard<std::mutex> lock(recording_session->mutex);

        Json::Value resp_json;
        resp_json["session_id"] = recording_session->session_id;
        resp_json["start_time"] = Json::Value::UInt64(recording_session->start_time);

        // Add markers
        Json::Value markers_json(Json::arrayValue);
        for (const auto& marker : recording_session->markers) {
            Json::Value m;
            m["timestamp"] = Json::Value::UInt64(marker.timestamp);
            m["marker_type"] = marker.marker_type;
            m["task_id"] = marker.task_id;
            markers_json.append(m);
        }
        resp_json["markers"] = markers_json;

        // Add samples
        Json::Value samples_json(Json::arrayValue);
        for (const auto& sample : recording_session->samples) {
            Json::Value s;
            s["timestamp"] = Json::Value::UInt64(sample.timestamp);
            s["stream_id"] = Json::Value::UInt64(sample.stream_id);
            s["sensor_position"] = sample.sensor_position;
            s["quat_i"] = sample.quat_i;
            s["quat_j"] = sample.quat_j;
            s["quat_k"] = sample.quat_k;
            s["quat_real"] = sample.quat_real;
            s["accel_x"] = sample.accel_x;
            s["accel_y"] = sample.accel_y;
            s["accel_z"] = sample.accel_z;
            s["gyro_x"] = sample.gyro_x;
            s["gyro_y"] = sample.gyro_y;
            s["gyro_z"] = sample.gyro_z;
            s["gravity_x"] = sample.gravity_x;
            s["gravity_y"] = sample.gravity_y;
            s["gravity_z"] = sample.gravity_z;
            samples_json.append(s);
        }
        resp_json["samples"] = samples_json;

        resp_json["marker_count"] = Json::Value::UInt(recording_session->markers.size());
        resp_json["sample_count"] = Json::Value::UInt(recording_session->samples.size());

        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }
};

int main()
{
    std::cout << "Starting Drogon server now..." << std::endl;

    // Set the number of threads for the application.
    // '0' means Drogon will use the number of CPU cores.
    //app().setThreadNum(8);

    std::cout << "Threads set\n";
    // Add a listener on 0.0.0.0:7409.
    // This allows connections from any network interface.
    app().addListener("0.0.0.0", 7409);
    std::cout << "Listening on port 7409\n";
    app().registerFilter(std::make_shared<CorsFilter>());
    app().registerPostHandlingAdvice(
        [](const drogon::HttpRequestPtr& req, const drogon::HttpResponsePtr& resp) {
            // Allow any localhost port for development
            auto origin = req->getHeader("Origin");
            if (!origin.empty()) {
                resp->addHeader("Access-Control-Allow-Origin", origin);
            }
        });

    const char* brokerHost = std::getenv("LIBNATKIT_KAFKA_BROKER_ADDRESS");
    if (brokerHost == nullptr || std::string(brokerHost).empty()) {
        brokerHost = "localhost";
    }

    const char* brokerPort = std::getenv("LIBNATKIT_KAFKA_BROKER_PORT");
    if (brokerPort == nullptr || std::string(brokerPort).empty()) {
        brokerPort = "29093";
    }

    std::cout << "Using Kafka broker at " << brokerHost << ":" << brokerPort << "\n";
    auto manager_unique = nat::kafka::createBrokerManager(brokerHost, brokerPort);
    
    if (manager_unique == nullptr) {
        std::cout << "Unable to create broker! Exiting\n";
        exit(1);
    }
    else {
        std::cout << "Broker created\n";
    }
    
    // Convert to shared_ptr so we can share between controllers
    std::shared_ptr<nat::kafka::BrokerManager> manager = std::move(manager_unique);
    
    Config config{};
    auto api_controller = std::make_shared<ApiController>(manager, config);
    std::cout << "API Controller created\n";
    
    // Set broker manager and register WebSocket controller
    StreamViewerWebSocket::setBrokerManager(manager);
    auto ws_controller = std::make_shared<StreamViewerWebSocket>();
    app().registerController(ws_controller);
    std::cout << "StreamViewer WebSocket controller registered\n";

    // Register the controller we defined above.
    // Drogon will automatically handle routing for it.
    //app().registerController(api_controller);
    // Register GET /api/heartbeat
    app().registerHandler("/api/heartbeat",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->heartbeat(req, std::move(callback));
        },
        { Get });

    // Register GET /api/is_connected_to_broker
    app().registerHandler("/api/is_connected_to_broker",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->is_connected_to_broker(req, std::move(callback));
        },
        { Get });

    // Register GET /api/get_all_streams
    app().registerHandler("/api/get_all_streams",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->get_all_streams(req, std::move(callback));
        },
        { Get });

    // Register GET /api/get_accuracies
    app().registerHandler("/api/get_accuracies",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->get_accuracies(req, std::move(callback));
        },
        { Get });

    // Register POST /api/set_streams
    app().registerHandler("/api/set_streams",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->set_streams(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/start_calibration
    app().registerHandler("/api/start_calibration",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->start_calibration(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/start_recording
    app().registerHandler("/api/start_recording",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->start_recording(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/stop_recording
    app().registerHandler("/api/stop_recording",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->stop_recording(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/insert_marker
    app().registerHandler("/api/insert_marker",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->insert_marker(req, std::move(callback));
        },
        { Post, Options });

    // Register GET /api/get_session_data
    app().registerHandler("/api/get_session_data",
        [api_controller](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                api_controller->get_session_data(req, std::move(callback));
        },
        { Get });

    // WebSocket endpoint at /ws/stream_viewer is registered above via registerController

    std::cout << "Server running on http://127.0.0.1:7409\n";
    std::cout << "WebSocket endpoint available at ws://127.0.0.1:7409/ws/stream_viewer\n";

    // Run the application's event loop.
    // This call will block until the application is terminated.
    app().run();

    return 0;
}
