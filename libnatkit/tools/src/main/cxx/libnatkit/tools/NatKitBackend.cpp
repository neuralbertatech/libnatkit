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

#include <librdkafka/rdkafka.h>
#include <librdkafka/rdkafkacpp.h>
#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
#include <libnatkit/util/Strings.hpp>
#include <libnatkit/util/Vectors.hpp>

#include <nlohmann/json.hpp>

#include <drogon/drogon.h>
#include <drogon/HttpFilter.h>
#include <iostream>

using namespace drogon;
using namespace std::chrono_literals;

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
            resp->addHeader("Access-Control-Allow-Origin", "http://localhost:5175");
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
    std::unique_ptr<nat::kafka::BrokerManager> manager;
    std::optional<std::vector<std::unique_ptr<nat::core::TopicMessenger>>> imuStreams;

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
            imuStreams = new_imu_streams;
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
    METHOD_LIST_END

    ApiController(std::unique_ptr<nat::kafka::BrokerManager> manager, Config config)
        : manager(std::move(manager)), config(config) {}

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

    // Handler for GET /api/get_accuracies
    // Stubs a request to get accuracy metrics.
    void get_accuracies(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        
        // TODO: Implement logic to fetch and calculate accuracies.
        Json::Value resp_json;
        resp_json["model_a"] = 0.95;
        resp_json["model_b"] = 0.91;
        resp_json["overall"] = 0.93;
        auto resp = HttpResponse::newHttpJsonResponse(resp_json);
        resp->setStatusCode(drogon::HttpStatusCode::k200OK);
        resp->setContentTypeCode(drogon::CT_APPLICATION_JSON);
        callback(resp);
    }

    // Handler for GET /api/get_all_streams
    // Stubs a request to list all available data streams.
    void get_all_streams(const HttpRequestPtr& req, std::function<void(const HttpResponsePtr&)>&& callback)
    {
        auto topicStrings = manager->getAllTopicStrings();
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
            resp->addHeader("Access-Control-Allow-Origin", "http://localhost:5175");
        });

    //auto brokerHost = "natkit-v0-kafka";
    //auto brokerHost = "localhost";
    auto brokerHost = "10.26.0.214";
    auto brokerPort = "29093";
    auto manager = nat::kafka::createBrokerManager(brokerHost, brokerPort);
    
    if (manager == nullptr) {
        std::cout << "Unable to create broker! Exiting\n";
        exit(1);
    }
    else {
        std::cout << "Broker created\n";
    }
    Config config{};
    auto api_controller = std::make_shared<ApiController>(std::move(manager), config);
    std::cout << "Controller created\n";

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

    std::cout << "Server running on http://127.0.0.1:7409\n";

    // Run the application's event loop.
    // This call will block until the application is terminated.
    app().run();

    return 0;
}
