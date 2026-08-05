#include <algorithm>
#include <array>
#include <chrono>
#include <cmath>
#include <deque>
#include <fstream>
#include <iostream>
#include <map>
#include <mutex>
#include <optional>
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
#include "Auth.hpp"
#include "ParquetExport.hpp"

#include <filesystem>

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

Json::Value authUserToJson(const AuthenticatedUser& user)
{
    Json::Value json;
    json["username"] = user.username;
    json["display_name"] = user.display_name;
    json["is_admin"] = user.is_admin;
    return json;
}

std::optional<Json::Value> getDescriptorJsonForSchemaName(
    const std::string& schema_name)
{
    auto descriptor_maybe =
        nat::core::DataSchemaDescriptorRegistry::getDefault().findBySchemaName(
            schema_name);
    if (!descriptor_maybe.has_value()) {
        return std::nullopt;
    }

    auto encoded = descriptor_maybe.value()->encodeToBytes(
        nat::core::SerializationType::Json);
    if (!encoded) {
        return std::nullopt;
    }

    Json::Value descriptor_json;
    Json::CharReaderBuilder builder;
    std::string errors;
    std::string payload(encoded->begin(), encoded->end());
    std::unique_ptr<Json::CharReader> reader(builder.newCharReader());
    if (!reader->parse(
            payload.data(),
            payload.data() + payload.size(),
            &descriptor_json,
            &errors)) {
        return std::nullopt;
    }

    return descriptor_json;
}

Json::Value authUserSummaryToJson(const AuthUserSummary& user)
{
    Json::Value json = authUserToJson(
        AuthenticatedUser{user.username, user.display_name, user.is_admin});
    json["enabled"] = user.enabled;
    json["shared_compute_access"] = user.shared_compute_access;
    json["created_at_us"] = Json::Value::UInt64(user.created_at_us);
    json["updated_at_us"] = Json::Value::UInt64(user.updated_at_us);
    json["password_updated_at_us"] = Json::Value::UInt64(user.password_updated_at_us);
    json["last_login_at_us"] = Json::Value::UInt64(user.last_login_at_us);
    return json;
}

HttpResponsePtr makeJsonResponse(
    const Json::Value& json,
    HttpStatusCode status = k200OK)
{
    auto resp = HttpResponse::newHttpJsonResponse(json);
    resp->setStatusCode(status);
    resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
    return resp;
}

std::optional<bool> optionalBoolField(
    const std::shared_ptr<Json::Value>& json,
    const std::string& key)
{
    if (!json || !json->isMember(key)) {
        return std::nullopt;
    }
    return (*json)[key].asBool();
}

std::optional<std::string> optionalStringField(
    const std::shared_ptr<Json::Value>& json,
    const std::string& key)
{
    if (!json || !json->isMember(key)) {
        return std::nullopt;
    }
    return (*json)[key].asString();
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
    int calibration_status_accelerometer;
    int calibration_status_gyroscope;
    int calibration_status_rotation;
    bool has_data_accelerometer;
    bool has_data_gyroscope;
    bool has_data_rotation;
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
    // Fold a sample's accuracies over what is already known, taking a sensor's
    // value ONLY if that sample actually carries that sensor.
    //
    // has_data used to be ignored here, so a sample that carried no gyro reported
    // gyro accuracy 0 -- indistinguishable from a measured "Unreliable". Since a
    // sample rarely carries all three sensors at once, whichever sensors were
    // absent from the sample that happened to be read got reported as Unreliable,
    // which made calibration look far worse than it was.
    ImuAccuracies foldAccuraciesFromImuData(nat::core::NatImuDataSchema* imuData,
                                            ImuAccuracies known) {
        if (imuData == nullptr) return known;
        if (imuData->wasDataSetForAcceleration()) {
            known.accelerometer =
                nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                    imuData->getAccelerationAccuracy());
        }
        if (imuData->wasDataSetForGryoscope()) {
            known.gyroscope =
                nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                    imuData->getGyroscopeAccuracy());
        }
        if (imuData->wasDataSetForRotation()) {
            known.rotation =
                nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                    imuData->getRotationAccuracy());
        }
        return known;
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
                        accuracies = foldAccuraciesFromImuData(imuData, accuracies);
                    } else {
                        // Try to cast to NatImuBulkDataSchema (bulk data)
                        nat::core::NatImuBulkDataSchema* bulkData = dynamic_cast<nat::core::NatImuBulkDataSchema*>(message.get());
                        if (bulkData != nullptr) {
                            // Fold the whole frame, oldest to newest, so each
                            // sensor reports the most recent sample that actually
                            // carried it. Reading only records->back() meant a
                            // frame whose last sample lacked (say) rotation
                            // reported rotation as Unreliable even though an
                            // earlier sample in the SAME frame had a good value.
                            std::unique_ptr<std::vector<nat::core::NatImuDataSchema>> records = bulkData->createImuRecords();
                            if (records && !records->empty()) {
                                for (auto& record : *records) {
                                    accuracies = foldAccuraciesFromImuData(&record, accuracies);
                                }
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
                    const auto descriptor_json =
                        getDescriptorJsonForSchemaName(dataTopic->schemaName);
                    if (descriptor_json.has_value()) {
                        topic["descriptor"] = descriptor_json.value();
                    }
                    topics.append(topic);
                }
                for (const auto& metaTopic : metaTopics) {
                    Json::Value topic;
                    topic["schema_name"] = metaTopic->schemaName;
                    topic["type"] = toString(metaTopic->type);
                    topic["serialization_type"] = toString(metaTopic->serializationType);
                    const auto descriptor_json =
                        getDescriptorJsonForSchemaName(metaTopic->schemaName);
                    if (descriptor_json.has_value()) {
                        topic["descriptor"] = descriptor_json.value();
                    }
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
                                    sample.calibration_status_accelerometer =
                                        nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                            record.getAccelerationAccuracy());
                                    sample.calibration_status_gyroscope =
                                        nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                            record.getGyroscopeAccuracy());
                                    sample.calibration_status_rotation =
                                        nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                            record.getRotationAccuracy());
                                    sample.has_data_accelerometer = record.wasDataSetForAcceleration();
                                    sample.has_data_gyroscope = record.wasDataSetForGryoscope();
                                    sample.has_data_rotation = record.wasDataSetForRotation();
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
                                sample.calibration_status_accelerometer =
                                    nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                        imuData->getAccelerationAccuracy());
                                sample.calibration_status_gyroscope =
                                    nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                        imuData->getGyroscopeAccuracy());
                                sample.calibration_status_rotation =
                                    nat::core::NatImuDataSchema::convertSensorAccuracyToInt(
                                        imuData->getRotationAccuracy());
                                sample.has_data_accelerometer = imuData->wasDataSetForAcceleration();
                                sample.has_data_gyroscope = imuData->wasDataSetForGryoscope();
                                sample.has_data_rotation = imuData->wasDataSetForRotation();
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
            s["calibration_status_accelerometer"] = sample.calibration_status_accelerometer;
            s["calibration_status_gyroscope"] = sample.calibration_status_gyroscope;
            s["calibration_status_rotation"] = sample.calibration_status_rotation;
            s["has_data_accelerometer"] = sample.has_data_accelerometer;
            s["has_data_gyroscope"] = sample.has_data_gyroscope;
            s["has_data_rotation"] = sample.has_data_rotation;
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
    const char* backendPortEnv = std::getenv("NATKIT_BACKEND_PORT");
    const int backendPort =
        (backendPortEnv != nullptr && std::string(backendPortEnv).size() > 0)
            ? std::stoi(backendPortEnv)
            : 7409;

    // Add a listener on 0.0.0.0:${backendPort}.
    // This allows connections from any network interface.
    app().addListener("0.0.0.0", backendPort);
    std::cout << "Listening on port " << backendPort << "\n";
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
    auto& auth_manager = AuthManager::instance();
    std::cout << "API Controller created\n";
    
    // Set broker manager and register WebSocket controller
    StreamViewerWebSocket::setBrokerManager(manager);
    auto ws_controller = std::make_shared<StreamViewerWebSocket>();
    app().registerController(ws_controller);
    std::cout << "StreamViewer WebSocket controller registered\n";

    // Register the controller we defined above.
    // Drogon will automatically handle routing for it.
    //app().registerController(api_controller);

    app().registerHandler("/api/auth/session",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                Json::Value resp_json;
                const auto user = auth_manager.authenticateRequest(req);
                resp_json["authenticated"] = user.has_value();
                resp_json["bootstrap_required"] = auth_manager.bootstrapRequired();
                resp_json["bootstrap_mode"] = auth_manager.bootstrapMode();
                if (user.has_value()) {
                    resp_json["user"] = authUserToJson(user.value());
                }
                callback(makeJsonResponse(resp_json));
        },
        { Get });

    app().registerHandler("/api/auth/bootstrap",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (req->getContentType() != drogon::ContentType::CT_APPLICATION_JSON) {
                    Json::Value err_json;
                    err_json["message"] = "Expected JSON content.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                auto json = req->getJsonObject();
                if (!json) {
                    Json::Value err_json;
                    err_json["message"] = "Invalid JSON payload.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                const auto result = auth_manager.bootstrapAdmin(
                    (*json)["username"].asString(),
                    json->isMember("display_name") ? (*json)["display_name"].asString() : std::string{},
                    (*json)["password"].asString(),
                    json->isMember("bootstrap_password")
                        ? (*json)["bootstrap_password"].asString()
                        : std::string{});

                Json::Value resp_json;
                resp_json["message"] = result.message;
                if (result.user.has_value()) {
                    resp_json["user"] = authUserToJson(result.user.value());
                }
                auto resp = makeJsonResponse(resp_json, result.status);
                auth_manager.applySessionCookie(result.session_token, resp);
                callback(resp);
        },
        { Post, Options });

    app().registerHandler("/api/auth/login",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (req->getContentType() != drogon::ContentType::CT_APPLICATION_JSON) {
                    Json::Value err_json;
                    err_json["message"] = "Expected JSON content.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                auto json = req->getJsonObject();
                if (!json) {
                    Json::Value err_json;
                    err_json["message"] = "Invalid JSON payload.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                const auto result = auth_manager.login(
                    (*json)["username"].asString(),
                    (*json)["password"].asString());
                Json::Value resp_json;
                resp_json["message"] = result.message;
                if (result.user.has_value()) {
                    resp_json["user"] = authUserToJson(result.user.value());
                }
                auto resp = makeJsonResponse(resp_json, result.status);
                auth_manager.applySessionCookie(result.session_token, resp);
                callback(resp);
        },
        { Post, Options });

    app().registerHandler("/api/auth/logout",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                Json::Value resp_json;
                resp_json["message"] = "Signed out.";
                auto resp = makeJsonResponse(resp_json);
                auth_manager.logout(req, resp);
                callback(resp);
        },
        { Post, Options });

    app().registerHandler("/api/admin/users",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                const auto user = auth_manager.authenticateRequest(req);
                if (!user.has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                if (!user->is_admin) {
                    Json::Value err_json;
                    err_json["message"] = "Admin access required.";
                    callback(makeJsonResponse(err_json, k403Forbidden));
                    return;
                }

                Json::Value resp_json;
                Json::Value users_json(Json::arrayValue);
                for (const auto& entry : auth_manager.listUsers()) {
                    users_json.append(authUserSummaryToJson(entry));
                }
                resp_json["users"] = users_json;
                callback(makeJsonResponse(resp_json));
        },
        { Get });

    app().registerHandler("/api/admin/users/create",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                const auto actor = auth_manager.authenticateRequest(req);
                if (!actor.has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                if (!actor->is_admin) {
                    Json::Value err_json;
                    err_json["message"] = "Admin access required.";
                    callback(makeJsonResponse(err_json, k403Forbidden));
                    return;
                }
                auto json = req->getJsonObject();
                if (!json) {
                    Json::Value err_json;
                    err_json["message"] = "Invalid JSON payload.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                const auto result = auth_manager.createUser(
                    actor->username,
                    (*json)["username"].asString(),
                    json->isMember("display_name") ? (*json)["display_name"].asString() : std::string{},
                    (*json)["password"].asString(),
                    json->isMember("is_admin") ? (*json)["is_admin"].asBool() : false,
                    !json->isMember("enabled") || (*json)["enabled"].asBool(),
                    json->isMember("shared_compute_access") && (*json)["shared_compute_access"].asBool());

                Json::Value resp_json;
                resp_json["message"] = result.message;
                if (result.user.has_value()) {
                    resp_json["user"] = authUserToJson(result.user.value());
                }
                callback(makeJsonResponse(resp_json, result.status));
        },
        { Post, Options });

    app().registerHandler("/api/admin/users/update",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                const auto actor = auth_manager.authenticateRequest(req);
                if (!actor.has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                if (!actor->is_admin) {
                    Json::Value err_json;
                    err_json["message"] = "Admin access required.";
                    callback(makeJsonResponse(err_json, k403Forbidden));
                    return;
                }
                auto json = req->getJsonObject();
                if (!json) {
                    Json::Value err_json;
                    err_json["message"] = "Invalid JSON payload.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                const auto result = auth_manager.updateUser(
                    actor->username,
                    (*json)["username"].asString(),
                    optionalStringField(json, "display_name"),
                    optionalStringField(json, "password"),
                    optionalBoolField(json, "is_admin"),
                    optionalBoolField(json, "enabled"),
                    optionalBoolField(json, "shared_compute_access"));

                Json::Value resp_json;
                resp_json["message"] = result.message;
                if (result.user.has_value()) {
                    resp_json["user"] = authUserToJson(result.user.value());
                }
                callback(makeJsonResponse(resp_json, result.status));
        },
        { Post, Options });

    app().registerHandler("/api/admin/users/delete",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                const auto actor = auth_manager.authenticateRequest(req);
                if (!actor.has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                if (!actor->is_admin) {
                    Json::Value err_json;
                    err_json["message"] = "Admin access required.";
                    callback(makeJsonResponse(err_json, k403Forbidden));
                    return;
                }
                auto json = req->getJsonObject();
                if (!json) {
                    Json::Value err_json;
                    err_json["message"] = "Invalid JSON payload.";
                    callback(makeJsonResponse(err_json, k400BadRequest));
                    return;
                }

                const auto result = auth_manager.deleteUser(
                    actor->username,
                    (*json)["username"].asString());
                Json::Value resp_json;
                resp_json["message"] = result.message;
                callback(makeJsonResponse(resp_json, result.status));
        },
        { Post, Options });

    // Register GET /api/heartbeat
    app().registerHandler("/api/heartbeat",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->heartbeat(req, std::move(callback));
        },
        { Get });

    // Register GET /api/is_connected_to_broker
    app().registerHandler("/api/is_connected_to_broker",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->is_connected_to_broker(req, std::move(callback));
        },
        { Get });

    // Register GET /api/export/parquet — materialize a stream to a Parquet file
    // and hand it straight back as a download.
    //
    // The "join" is upstream on the canvas: a combine node fans a data stream
    // and an experiment's `markers` into ONE channel (Data/<id> + Marker/<id>
    // share a stream id), so pointing this at that channel gives rows labelled
    // with the active cue. A plain data stream exports too, just unlabelled.
    app().registerHandler("/api/export/parquet",
        [manager, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }

                const auto fail = [&callback](const std::string& message,
                                              HttpStatusCode code) {
                    Json::Value err_json;
                    err_json["message"] = message;
                    callback(makeJsonResponse(err_json, code));
                };

                if (!natkit::tools::parquetExportAvailable()) {
                    fail("This backend was built without Parquet support "
                         "(libparquet-dev missing at build time).",
                         k501NotImplemented);
                    return;
                }

                natkit::tools::ParquetExportRequest export_request;
                const auto stream_id_param = req->getParameter("stream_id");
                if (stream_id_param.empty()) {
                    fail("stream_id is required.", k400BadRequest);
                    return;
                }
                try {
                    export_request.streamId =
                        std::stoull(stream_id_param);
                } catch (const std::exception&) {
                    fail("stream_id must be a positive integer.", k400BadRequest);
                    return;
                }

                // Optional: markers from a different channel, so an export node
                // can take a data stream and an experiment as separate inputs
                // rather than requiring a combine node to bundle them first.
                const auto marker_stream_param =
                    req->getParameter("marker_stream_id");
                if (!marker_stream_param.empty()) {
                    try {
                        export_request.markerStreamId =
                            std::stoull(marker_stream_param);
                    } catch (const std::exception&) {
                        fail("marker_stream_id must be a positive integer.",
                             k400BadRequest);
                        return;
                    }
                }

                const auto parse_optional_i64 =
                    [&req](const char* name) -> std::optional<int64_t> {
                    const auto raw = req->getParameter(name);
                    if (raw.empty()) {
                        return std::nullopt;
                    }
                    try {
                        return std::stoll(raw);
                    } catch (const std::exception&) {
                        return std::nullopt;
                    }
                };
                export_request.startUs = parse_optional_i64("start_us");
                export_request.endUs = parse_optional_i64("end_us");

                const auto label_field = req->getParameter("label_field");
                if (!label_field.empty()) {
                    export_request.labelField = label_field;
                }
                // Tunables for a slow broker / large session.
                const auto first_record_timeout_ms =
                    parse_optional_i64("first_record_timeout_ms");
                if (first_record_timeout_ms.has_value()) {
                    export_request.firstRecordTimeoutMs =
                        static_cast<int>(first_record_timeout_ms.value());
                }
                const auto idle_timeout_ms = parse_optional_i64("idle_timeout_ms");
                if (idle_timeout_ms.has_value()) {
                    export_request.idleTimeoutMs =
                        static_cast<int>(idle_timeout_ms.value());
                }

                const auto run_index = req->getParameter("run_index");
                if (!run_index.empty()) {
                    try {
                        export_request.runIndex = std::stoi(run_index);
                    } catch (const std::exception&) {
                        fail("run_index must be an integer.", k400BadRequest);
                        return;
                    }
                }

                // Written to a temp dir, read back, then unlinked — the file is
                // an implementation detail, the download is the product.
                const std::string temp_dir =
                    std::getenv("NATKIT_EXPORT_TMPDIR") != nullptr
                        ? std::getenv("NATKIT_EXPORT_TMPDIR")
                        : "/tmp";
                const auto result = natkit::tools::exportStreamToParquet(
                    manager, export_request, temp_dir);
                if (!result.ok) {
                    LOG_WARN << "Parquet export failed for stream "
                             << export_request.streamId << ": " << result.error;
                    fail(result.error, k404NotFound);
                    return;
                }

                std::string body;
                {
                    std::ifstream file(result.filePath,
                                       std::ios::binary | std::ios::ate);
                    if (!file) {
                        fail("Export succeeded but the file could not be read.",
                             k500InternalServerError);
                        std::remove(result.filePath.c_str());
                        return;
                    }
                    const auto size = file.tellg();
                    file.seekg(0, std::ios::beg);
                    body.resize(static_cast<size_t>(size));
                    file.read(body.data(), size);
                }
                std::remove(result.filePath.c_str());

                auto resp = HttpResponse::newHttpResponse();
                resp->setStatusCode(k200OK);
                resp->setContentTypeString("application/vnd.apache.parquet");
                resp->setBody(std::move(body));
                resp->addHeader("Content-Disposition",
                                "attachment; filename=\"" + result.fileName + "\"");
                // Surfaced in the UI after the download so the operator can see
                // what they actually got without opening the file.
                resp->addHeader("X-Natkit-Frame-Count",
                                std::to_string(result.frameCount));
                resp->addHeader("X-Natkit-Labelled-Frame-Count",
                                std::to_string(result.labelledFrameCount));
                resp->addHeader("X-Natkit-Marker-Count",
                                std::to_string(result.markerCount));
                resp->addHeader("X-Natkit-Truncated",
                                result.truncated ? "true" : "false");
                if (!result.sessionId.empty()) {
                    resp->addHeader("X-Natkit-Session-Id", result.sessionId);
                }
                LOG_INFO << "Parquet export: stream " << export_request.streamId
                         << " -> " << result.frameCount << " frames ("
                         << result.labelledFrameCount << " labelled), "
                         << result.markerCount << " markers";
                callback(resp);
        },
        { Get });

    // --- Experiment media -------------------------------------------------
    // Cue images and sounds a user uploads while authoring an experiment. They
    // live beside the experiment store in the graphs volume, because media IS
    // experiment configuration: it is persisted, backed up and restored with the
    // protocol that references it, rather than needing its own volume.
    //
    // The client never sees or supplies a filesystem path. It uploads bytes and
    // gets an opaque id back; the id is all that is stored in the protocol.
    const auto mediaDirectory = []() -> std::filesystem::path {
        if (const char* explicit_dir = std::getenv("NATKIT_MEDIA_DIR")) {
            if (*explicit_dir != '\0') {
                return std::filesystem::path(explicit_dir);
            }
        }
        // Default: alongside the stream-graph store.
        if (const char* store = std::getenv("NATKIT_STREAM_GRAPH_STORE")) {
            if (*store != '\0') {
                return std::filesystem::path(store).parent_path() / "media";
            }
        }
        return std::filesystem::path("/graphs/media");
    }();

    // An id is generated by US, so it can be validated strictly: anything with a
    // slash or a dot-dot never reaches the filesystem.
    const auto mediaIdIsSafe = [](const std::string& id) {
        if (id.empty() || id.size() > 128) {
            return false;
        }
        bool seen_dot = false;
        for (const char c : id) {
            const bool ok = (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') ||
                            (c >= '0' && c <= '9') || c == '-' || c == '_' ||
                            c == '.';
            if (!ok) {
                return false;
            }
            if (c == '.') {
                seen_dot = true;
            }
        }
        return seen_dot && id.find("..") == std::string::npos;
    };

    // Only formats a browser can actually show or play, keyed off the extension
    // so serving never has to sniff bytes.
    const auto mediaContentType =
        [](const std::string& extension) -> std::string {
        static const std::map<std::string, std::string> types{
            {"png", "image/png"},   {"jpg", "image/jpeg"},
            {"jpeg", "image/jpeg"}, {"gif", "image/gif"},
            {"webp", "image/webp"}, {"svg", "image/svg+xml"},
            {"bmp", "image/bmp"},   {"wav", "audio/wav"},
            {"mp3", "audio/mpeg"},  {"ogg", "audio/ogg"},
            {"oga", "audio/ogg"},   {"m4a", "audio/mp4"},
            {"aac", "audio/aac"},   {"flac", "audio/flac"},
            {"webm", "audio/webm"},
        };
        std::string lower;
        for (const char c : extension) {
            lower.push_back(static_cast<char>(std::tolower(c)));
        }
        const auto found = types.find(lower);
        return found == types.end() ? std::string{} : found->second;
    };

    // Register POST /api/media — upload a cue image or sound.
    app().registerHandler("/api/media",
        [&auth_manager, mediaDirectory, mediaContentType](
            const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                const auto fail = [&callback](const std::string& message,
                                              HttpStatusCode code) {
                    Json::Value err_json;
                    err_json["message"] = message;
                    callback(makeJsonResponse(err_json, code));
                };

                MultiPartParser parser;
                if (parser.parse(req) != 0 || parser.getFiles().empty()) {
                    fail("Expected a multipart upload with one file.",
                         k400BadRequest);
                    return;
                }
                const auto& file = parser.getFiles()[0];

                // 25 MB. A cue image or a spoken prompt is far smaller; the limit
                // exists so a mis-drag cannot fill the volume.
                constexpr size_t kMaxBytes = 25u * 1024u * 1024u;
                if (file.fileLength() == 0) {
                    fail("The uploaded file is empty.", k400BadRequest);
                    return;
                }
                if (file.fileLength() > kMaxBytes) {
                    fail("That file is larger than the 25 MB limit.",
                         k413RequestEntityTooLarge);
                    return;
                }

                const auto extension = file.getFileExtension();
                const std::string extension_str(extension);
                const auto content_type = mediaContentType(extension_str);
                if (content_type.empty()) {
                    fail("Unsupported media type '." + extension_str +
                             "'. Use png/jpg/gif/webp/svg for images or "
                             "wav/mp3/ogg/m4a/flac/webm for sound.",
                         k415UnsupportedMediaType);
                    return;
                }

                std::error_code directory_error;
                std::filesystem::create_directories(mediaDirectory,
                                                    directory_error);
                if (directory_error) {
                    fail("Could not create the media directory: " +
                             directory_error.message(),
                         k500InternalServerError);
                    return;
                }

                // Opaque, collision-resistant, and carrying the extension so
                // serving can set a content type without sniffing.
                static std::mt19937_64 id_engine{std::random_device{}()};
                std::ostringstream id_stream;
                const auto now_us =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::system_clock::now().time_since_epoch())
                        .count();
                id_stream << std::hex << now_us << "-"
                          << (id_engine() & 0xffffffffull) << "."
                          << extension_str;
                const std::string media_id = id_stream.str();

                const auto destination = mediaDirectory / media_id;
                std::ofstream out(destination, std::ios::binary);
                if (!out) {
                    fail("Could not write to the media directory.",
                         k500InternalServerError);
                    return;
                }
                out.write(file.fileContent().data(),
                          static_cast<std::streamsize>(file.fileLength()));
                out.close();
                if (!out) {
                    fail("Writing the uploaded file failed.",
                         k500InternalServerError);
                    return;
                }

                LOG_INFO << "Stored experiment media " << media_id << " ("
                         << file.fileLength() << " bytes, " << content_type
                         << ")";
                Json::Value resp_json;
                resp_json["media_id"] = media_id;
                resp_json["url"] = "/api/media/" + media_id;
                resp_json["content_type"] = content_type;
                resp_json["bytes"] = static_cast<Json::UInt64>(file.fileLength());
                resp_json["original_name"] = std::string(file.getFileName());
                callback(makeJsonResponse(resp_json, k200OK));
        },
        {Post});

    // Register GET /api/media/{id} — serve an uploaded cue image or sound.
    app().registerHandler("/api/media/{id}",
        [mediaDirectory, mediaIdIsSafe, mediaContentType](
            const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback,
            const std::string& id) {
                // Deliberately NOT auth-gated: an <img>/<audio> element cannot
                // carry the session header, and these are opaque, unguessable
                // ids for participant-facing stimuli.
                const auto fail = [&callback](const std::string& message,
                                              HttpStatusCode code) {
                    Json::Value err_json;
                    err_json["message"] = message;
                    callback(makeJsonResponse(err_json, code));
                };
                if (!mediaIdIsSafe(id)) {
                    fail("Malformed media id.", k400BadRequest);
                    return;
                }
                const auto dot = id.rfind('.');
                const auto content_type =
                    mediaContentType(dot == std::string::npos
                                         ? std::string{}
                                         : id.substr(dot + 1));
                if (content_type.empty()) {
                    fail("Unsupported media type.", k415UnsupportedMediaType);
                    return;
                }

                const auto full = mediaDirectory / id;
                std::ifstream file(full, std::ios::binary | std::ios::ate);
                if (!file) {
                    fail("No such media.", k404NotFound);
                    return;
                }
                std::string body;
                const auto size = file.tellg();
                file.seekg(0, std::ios::beg);
                body.resize(static_cast<size_t>(size));
                file.read(body.data(), size);

                auto resp = HttpResponse::newHttpResponse();
                resp->setStatusCode(k200OK);
                resp->setContentTypeString(content_type);
                resp->setBody(std::move(body));
                // Content-addressed enough to cache hard: an id is never reused.
                resp->addHeader("Cache-Control", "public, max-age=31536000, immutable");
                callback(resp);
        },
        {Get});

    // Register GET /api/instances/artifact — download a MATERIALIZED instance
    // artifact straight off disk.
    //
    // Deliberately not the /api/export/parquet path: that one re-drains Kafka, and
    // the entire point of an instance is that its data has LEFT Kafka (retention is
    // 168h). Reviewing a snapshot from a year ago has to read the file, not the
    // broker.
    app().registerHandler("/api/instances/artifact",
        [&auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                const auto fail = [&callback](const std::string& message,
                                              HttpStatusCode code) {
                    Json::Value err_json;
                    err_json["message"] = message;
                    callback(makeJsonResponse(err_json, code));
                };

                const auto graph_id = req->getParameter("graph_id");
                const auto name = req->getParameter("path");
                if (graph_id.empty() || name.empty()) {
                    fail("graph_id and path are required.", k400BadRequest);
                    return;
                }

                // The artifact directory comes from the STORE, and `path` is
                // matched against the manifest rather than joined onto the
                // directory: a client-supplied path would otherwise be a directory
                // traversal ("../../etc/passwd") straight out of the volume.
                std::string directory;
                bool listed = false;
                if (!natkitLookupInstanceArtifact(graph_id, name, directory,
                                                  listed)) {
                    fail("Unknown instance graph: " + graph_id, k404NotFound);
                    return;
                }
                if (!listed) {
                    fail("'" + name + "' is not an artifact of this instance.",
                         k404NotFound);
                    return;
                }

                const std::filesystem::path full =
                    std::filesystem::path(directory) / name;
                std::ifstream file(full, std::ios::binary | std::ios::ate);
                if (!file) {
                    fail("The artifact is missing from disk: " + full.string() +
                         " — the instance record and the store disagree.",
                         k410Gone);
                    return;
                }
                std::string body;
                const auto size = file.tellg();
                file.seekg(0, std::ios::beg);
                body.resize(static_cast<size_t>(size));
                file.read(body.data(), size);

                auto resp = HttpResponse::newHttpResponse();
                resp->setStatusCode(k200OK);
                const bool is_parquet = name.size() > 8 &&
                    name.compare(name.size() - 8, 8, ".parquet") == 0;
                resp->setContentTypeString(
                    is_parquet ? "application/vnd.apache.parquet"
                               : "application/x-ndjson");
                resp->setBody(std::move(body));
                resp->addHeader("Content-Disposition",
                                "attachment; filename=\"" + graph_id + "-" + name +
                                    "\"");
                LOG_INFO << "Served instance artifact " << graph_id << "/" << name
                         << " (" << size << " bytes)";
                callback(resp);
        },
        { Get });

    // Register GET /api/get_all_streams
    app().registerHandler("/api/get_all_streams",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->get_all_streams(req, std::move(callback));
        },
        { Get });

    // Register GET /api/get_accuracies
    app().registerHandler("/api/get_accuracies",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->get_accuracies(req, std::move(callback));
        },
        { Get });

    // Register POST /api/set_streams
    app().registerHandler("/api/set_streams",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->set_streams(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/start_calibration
    app().registerHandler("/api/start_calibration",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->start_calibration(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/start_recording
    app().registerHandler("/api/start_recording",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->start_recording(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/stop_recording
    app().registerHandler("/api/stop_recording",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->stop_recording(req, std::move(callback));
        },
        { Post, Options });

    // Register POST /api/insert_marker
    app().registerHandler("/api/insert_marker",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->insert_marker(req, std::move(callback));
        },
        { Post, Options });

    // Register GET /api/get_session_data
    app().registerHandler("/api/get_session_data",
        [api_controller, &auth_manager](const HttpRequestPtr& req,
            std::function<void(const HttpResponsePtr&)>&& callback) {
                if (!auth_manager.authenticateRequest(req).has_value()) {
                    Json::Value err_json;
                    err_json["message"] = "Authentication required.";
                    callback(makeJsonResponse(err_json, k401Unauthorized));
                    return;
                }
                api_controller->get_session_data(req, std::move(callback));
        },
        { Get });

    // WebSocket endpoint at /ws/stream_viewer is registered above via registerController

    std::cout << "Server running on http://127.0.0.1:" << backendPort << "\n";
    std::cout << "WebSocket endpoint available at ws://127.0.0.1:" << backendPort
              << "/ws/stream_viewer\n";

    // Run the application's event loop.
    // This call will block until the application is terminated.
    app().run();

    return 0;
}
