#pragma once

#include <drogon/WebSocketController.h>
#include <drogon/HttpAppFramework.h>
#include <libnatkit-kafka.hpp>
#include <libnatkit-core.hpp>
#include <nlohmann/json.hpp>
#include <set>
#include <map>
#include <mutex>
#include <thread>
#include <atomic>
#include <memory>

// Client context for tracking subscriptions and streaming thread
struct StreamViewerClientContext {
    std::set<uint64_t> subscribed_streams;
    std::vector<std::unique_ptr<nat::core::TopicMessenger>> messengers;
    std::atomic<bool> active{true};
    std::thread streaming_thread;
    std::mutex mutex;

    ~StreamViewerClientContext() {
        active = false;
        if (streaming_thread.joinable()) {
            streaming_thread.join();
        }
    }
};

class StreamViewerWebSocket : public drogon::WebSocketController<StreamViewerWebSocket, false>
{
public:
    StreamViewerWebSocket() = default;
    ~StreamViewerWebSocket() = default;

    // WebSocket event handlers
    virtual void handleNewMessage(const drogon::WebSocketConnectionPtr& conn,
                                  std::string&& message,
                                  const drogon::WebSocketMessageType& type) override;
    
    virtual void handleConnectionClosed(const drogon::WebSocketConnectionPtr& conn) override;
    
    virtual void handleNewConnection(const drogon::HttpRequestPtr& req,
                                     const drogon::WebSocketConnectionPtr& conn) override;

    // Path registration via macro
    WS_PATH_LIST_BEGIN
    WS_PATH_ADD("/ws/stream_viewer");
    WS_PATH_LIST_END

    // Set the broker manager (must be called before app starts)
    static void setBrokerManager(std::shared_ptr<nat::kafka::BrokerManager> manager) {
        broker_manager_ = manager;
    }

private:
    static std::shared_ptr<nat::kafka::BrokerManager> broker_manager_;
    std::map<drogon::WebSocketConnectionPtr, std::unique_ptr<StreamViewerClientContext>> clients_;
    std::mutex clients_mutex_;

    // Handle subscribe action
    void handleSubscribe(const drogon::WebSocketConnectionPtr& conn,
                         StreamViewerClientContext* ctx,
                         const std::vector<uint64_t>& stream_ids);

    // Handle unsubscribe action
    void handleUnsubscribe(const drogon::WebSocketConnectionPtr& conn,
                           StreamViewerClientContext* ctx,
                           const std::vector<uint64_t>& stream_ids);

    // Send stream list to client
    void sendStreamList(const drogon::WebSocketConnectionPtr& conn);

    // Background thread function that reads from Kafka and pushes to WebSocket
    void streamingThreadFunc(const drogon::WebSocketConnectionPtr& conn,
                             StreamViewerClientContext* ctx);

    // Format NatImuDataSchema as JSON
    nlohmann::json formatImuDataAsJson(const nat::core::NatImuDataSchema& data,
                                       uint64_t stream_id,
                                       const std::string& encoding_type,
                                       size_t encoding_size);

    // Format NatImuBulkDataSchema as JSON
    nlohmann::json formatBulkDataAsJson(const nat::core::NatImuBulkDataSchema& bulk,
                                        uint64_t stream_id,
                                        const std::string& encoding_type,
                                        size_t encoding_size);

    // Format NatMuseDataSchema as JSON
    nlohmann::json formatMuseDataAsJson(const nat::core::NatMuseDataSchema& data,
                                        uint64_t stream_id,
                                        const std::string& encoding_type,
                                        size_t encoding_size);

    // Format NatMuseBulkDataSchema as JSON
    nlohmann::json formatMuseBulkDataAsJson(const nat::core::NatMuseBulkDataSchema& bulk,
                                            uint64_t stream_id,
                                            const std::string& encoding_type,
                                            size_t encoding_size);

    // Send error message to client
    void sendError(const drogon::WebSocketConnectionPtr& conn, const std::string& message);

    // Send status update to client
    void sendStatus(const drogon::WebSocketConnectionPtr& conn, StreamViewerClientContext* ctx);
};
