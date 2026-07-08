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

#include "Auth.hpp"

// Client context for tracking subscriptions and streaming thread
struct StreamViewerClientContext {
    std::set<uint64_t> subscribed_streams;
    std::vector<std::unique_ptr<nat::core::TopicMessenger>> messengers;
    std::atomic<bool> active{true};
    std::thread streaming_thread;
    std::mutex mutex;
    std::optional<AuthenticatedUser> user;

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

    // Publish experiment metadata and markers to Kafka
    void handlePublishSessionBundle(const drogon::WebSocketConnectionPtr& conn,
                                    const nlohmann::json& json);

    void handleListTransformCapabilities(
        const drogon::WebSocketConnectionPtr& conn,
        const nlohmann::json& json);

    void handleListNodeCatalog(
        const drogon::WebSocketConnectionPtr& conn,
        const nlohmann::json& json);

    // Create a derived EMG transform stream
    void handleCreateTransform(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

    // List active transform workers
    void handleListTransforms(const drogon::WebSocketConnectionPtr& conn,
                              const nlohmann::json& json);

    // Stop a derived transform stream
    void handleStopTransform(const drogon::WebSocketConnectionPtr& conn,
                             const nlohmann::json& json);

    void handleListStreamGraphs(const drogon::WebSocketConnectionPtr& conn,
                                const nlohmann::json& json);

    void handleSaveStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

    void handleValidateStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                                   const nlohmann::json& json);

    void handleGetStreamGraphStatus(const drogon::WebSocketConnectionPtr& conn,
                                    const nlohmann::json& json);

    void handleStartStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                                const nlohmann::json& json);

    void handleStopStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

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

    // Format ExgPillEmgDataSchemaV1 as Stream Viewer JSON
    nlohmann::json formatEmgDataAsJson(const nat::core::ExgPillEmgDataSchemaV1& data,
                                       uint64_t stream_id,
                                       const std::string& encoding_type,
                                       size_t encoding_size);

    // Format ExgPillEmgTransformDataSchemaV1 as Stream Viewer JSON
    nlohmann::json formatEmgDataAsJson(
        const nat::core::ExgPillEmgTransformDataSchemaV1& data,
        uint64_t stream_id,
        const std::string& encoding_type,
        size_t encoding_size);

    nlohmann::json formatEmgDataAsJson(
        const nat::core::NatSignalFrameDataSchemaV1& data,
        uint64_t stream_id,
        const std::string& encoding_type,
        size_t encoding_size);

    nlohmann::json formatTransformProvenanceAsJson(
        const nat::core::TransformProvenanceRecord& record,
        uint64_t stream_id,
        const std::string& encoding_type,
        size_t encoding_size);

    // Send error message to client
    void sendError(const drogon::WebSocketConnectionPtr& conn, const std::string& message);

    // Send status update to client
    void sendStatus(const drogon::WebSocketConnectionPtr& conn, StreamViewerClientContext* ctx);

    // Send publish acknowledgement to client
    void sendPublishResult(const drogon::WebSocketConnectionPtr& conn,
                           const std::string& request_id,
                           const std::string& session_id,
                           size_t published_meta_records,
                           size_t published_marker_events);

    void sendTransformCapabilities(
        const drogon::WebSocketConnectionPtr& conn,
        const std::string& request_id);

    void sendNodeCatalog(
        const drogon::WebSocketConnectionPtr& conn,
        const std::string& request_id);

    void sendTransformResult(const drogon::WebSocketConnectionPtr& conn,
                             const std::string& request_id,
                             uint64_t source_stream_id,
                             uint64_t output_stream_id,
                             const std::string& output_identifier,
                             const std::string& transform_kind,
                             const std::string& input_mapping_id,
                             const std::string& topic,
                             const std::string& worker_id,
                             const std::string& thread_slot_id,
                             size_t slot_capacity,
                             size_t active_count,
                             bool already_exists);

    void sendTransformList(const drogon::WebSocketConnectionPtr& conn,
                           const std::string& request_id);

    void sendStreamGraphList(const drogon::WebSocketConnectionPtr& conn,
                             const std::string& request_id);

    void sendStreamGraphSaved(const drogon::WebSocketConnectionPtr& conn,
                              const std::string& request_id,
                              const nlohmann::json& graph_json);

    void sendStreamGraphValidation(
        const drogon::WebSocketConnectionPtr& conn,
        const std::string& request_id,
        const std::string& graph_id,
        bool valid,
        const nlohmann::json& graph_diagnostics,
        const nlohmann::json& node_diagnostics,
        const nlohmann::json& edge_diagnostics);

    void sendStreamGraphStatus(const drogon::WebSocketConnectionPtr& conn,
                               const std::string& request_id,
                               const std::string& graph_id);

    void sendStreamGraphStarted(const drogon::WebSocketConnectionPtr& conn,
                                const std::string& request_id,
                                const std::string& graph_id);

    void sendStreamGraphStopped(const drogon::WebSocketConnectionPtr& conn,
                                const std::string& request_id,
                                const std::string& graph_id);

    void broadcastTransformList();
};
