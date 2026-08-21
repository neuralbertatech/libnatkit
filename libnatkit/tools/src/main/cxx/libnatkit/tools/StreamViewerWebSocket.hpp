#pragma once

#include <drogon/WebSocketController.h>
#include <drogon/HttpAppFramework.h>
#include <drogon/WebSocketClient.h>
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
#include "ReplaySource.hpp"

// A record projected onto the canonical numeric channel-frame shape.
//
// Shared so the Parquet exporter runs the SAME projection as the transform and
// viewer paths rather than reimplementing it. That matters beyond tidiness: a
// sensor whose descriptor doesn't literally match the contract (the IMU bulk
// frame exposes flat accel_x/gyro_z arrays, Muse nests under eeg.*) is only
// filterable/plottable because of an alternate input mapping, and export has to
// honour those same mappings or it silently supports a narrower set of sensors
// than the rest of the system.
struct NatKitChannelFrameProjection {
    std::string deviceId;
    uint64_t seqNo = 0;
    uint64_t deviceTsUs = 0;
    uint32_t sampleRateHz = 0;
    std::vector<std::string> channelLabels;
    // Channel-major: channelLabels.size() runs of samplesPerChannel floats.
    std::vector<float> samples;
    uint32_t samplesPerChannel = 0;
};

// Project a record to channel frames via the canonical contract, falling back
// to any registered alternate input mapping for its schema. nullopt when the
// record isn't a numeric channel frame under either.
// sourceStreamId is used to synthesize a device id for schemas that carry no
// device_id field of their own (the IMU bulk frame is one), so pass the real
// stream id -- it ends up in the exported rows and the download filename.
std::optional<NatKitChannelFrameProjection> projectRecordToChannelFrame(
    const nat::core::Schema& record, uint64_t sourceStreamId = 0);

// Resolve an instance artifact for download: given an instance's graph id and an
// artifact name, return the directory holding it and whether that name is actually
// listed in the instance's manifest.
//
// The name is CHECKED AGAINST THE MANIFEST rather than joined onto the directory by
// the caller, because a client-supplied path would otherwise be a directory
// traversal straight out of the instance volume. Lives here because the graph store
// (and therefore the instance record) does.
// Returns false when the graph id is not an instance.
bool natkitLookupInstanceArtifact(const std::string& graphId,
                                  const std::string& artifactName,
                                  std::string& directoryOut,
                                  bool& listedOut);

// Client context for tracking subscriptions and streaming thread
struct StreamViewerClientContext {
    std::set<uint64_t> subscribed_streams;
    std::vector<std::unique_ptr<nat::core::TopicMessenger>> messengers;
    // Requested consumer start offset per subscribed stream (Phase 3 — historical
    // reads). -1 = live tail (OFFSET_END, default), -2 = OFFSET_BEGINNING, >=0 a
    // concrete offset (e.g. from offsets_for_times when scrubbing to a timestamp).
    // Consulted by both the immediate bind and the lazy-bind path.
    std::map<uint64_t, int64_t> requested_start_offsets;
    // Topic-aware channels: a channel id can bind BOTH a DATA and a MARKER topic
    // (a combine "stream" output). Track which topic types have been bound per
    // stream id so the lazy-bind path can still attach the marker topic after the
    // data topic (or vice versa) once it materialises — both share one id, so a
    // messenger-count check can't tell them apart.
    std::set<uint64_t> bound_data_ids;
    std::set<uint64_t> bound_marker_ids;
    std::atomic<bool> active{true};
    std::thread streaming_thread;
    // Device health (TEC-NATKIT-33) is its own thread, not part of the streaming
    // one: it tails LOGGING_LOG status topics that no graph subscribes to, on its
    // own 1 s cadence, and it must keep working while nothing is running --
    // "is the rig alive?" is asked most often when no board is streaming.
    std::atomic<bool> device_health_active{false};
    std::thread device_health_thread;
    std::mutex mutex;
    std::optional<AuthenticatedUser> user;

    ~StreamViewerClientContext() {
        active = false;
        device_health_active = false;
        if (streaming_thread.joinable()) {
            streaming_thread.join();
        }
        if (device_health_thread.joinable()) {
            device_health_thread.join();
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

    // --- ML control-plane proxy (Phase 5, decision #3) ---
    // The backend is the SOLE client of the Python ML control plane (:8786); the
    // browser no longer connects there directly. Browser ML actions arrive on
    // /ws/stream_viewer wrapped as {"action":"ml_proxy","message":<cp-action>}
    // and are forwarded up; control-plane messages are re-broadcast down wrapped
    // as {"type":"ml_control_plane","message":<cp-message>}.
    drogon::WebSocketClientPtr ml_client_;
    std::mutex ml_client_mutex_;
    bool ml_client_connecting_ = false;

    // Lazily (re)connect the outbound control-plane client.
    void ensureMlControlPlaneClient();
    // Forward a browser-originated control-plane action up to the control plane.
    void handleMlProxyAction(const drogon::WebSocketConnectionPtr& conn,
                             const nlohmann::json& json);
    // Swap instance graph ids in a train job for their materialized artifact paths
    // (Phase 6). The backend owns the instance store, so it is the only component
    // that can map an instance to files — the browser must not know container paths,
    // and the control plane must not read the graph store.
    bool resolveTrainInstanceDatasets(nlohmann::json& message, std::string& error);

    // Re-broadcast a raw control-plane message to all /ws/stream_viewer clients.
    void broadcastMlControlPlaneMessage(const std::string& raw_message);

    // Handle subscribe action. start_offset selects where each stream's consumer
    // begins: -1 live tail (default), -2 OFFSET_BEGINNING, >=0 a concrete offset.
    void handleSubscribe(const drogon::WebSocketConnectionPtr& conn,
                         StreamViewerClientContext* ctx,
                         const std::vector<uint64_t>& stream_ids,
                         int64_t start_offset = -1);

    // Handle a time-introspection query: earliest/latest offset for a stream and
    // (optionally) the offset for a given timestamp (offsets_for_times). Lets the
    // UI know a stream's available history extent and map a scrubbed timestamp to
    // a start offset. (Phase 3.)
    void handleQueryStreamTime(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

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

    // Fork a graph into a new EDITABLE instance nested under the same experiment.
    // A fork is an instance that happens to be mutable: it inherits the source's
    // `recording` verbatim, so it points at the same materialized artifacts and
    // forking is free regardless of how large the recording was.
    void handleForkStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

    void handleValidateStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                                   const nlohmann::json& json);

    void handleGetStreamGraphStatus(const drogon::WebSocketConnectionPtr& conn,
                                    const nlohmann::json& json);

    void handleStartStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                                const nlohmann::json& json);

    void handleStopStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);

    // Incremental reactivity (Phase 7): restart one node + its downstream
    // subgraph in a running graph after a config change.
    void handleRestartStreamGraphNode(const drogon::WebSocketConnectionPtr& conn,
                                      const nlohmann::json& json);

    // Individual profiles (Phase 4): resume a person's live classify graph.
    void handleListProfiles(const drogon::WebSocketConnectionPtr& conn,
                            const nlohmann::json& json);
    void handleSaveProfile(const drogon::WebSocketConnectionPtr& conn,
                           const nlohmann::json& json);
    void handleDeleteProfile(const drogon::WebSocketConnectionPtr& conn,
                             const nlohmann::json& json);

    // Experiments (experiment-history-snapshots-plan, Phase 1): the experiment is
    // a stored entity that owns a board and (later) its recorded instances, so the
    // protocol/participant/notes leave the canvas. save_experiment is also the
    // sole writer of the 1:1 experiment<->board binding.
    // Workspaces (TEC-NATKIT-56): a container that scopes the experiment picker.
    void handleListWorkspaces(const drogon::WebSocketConnectionPtr& conn,
                              const nlohmann::json& json);
    void handleSaveWorkspace(const drogon::WebSocketConnectionPtr& conn,
                             const nlohmann::json& json);
    void handleDeleteWorkspace(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);
    void sendWorkspaceList(const drogon::WebSocketConnectionPtr& conn,
                           const std::string& request_id);
    void sendWorkspaceSaved(const drogon::WebSocketConnectionPtr& conn,
                            const std::string& request_id,
                            const nlohmann::json& workspace_json);
    void sendWorkspaceDeleted(const drogon::WebSocketConnectionPtr& conn,
                              const std::string& request_id,
                              const std::string& workspace_id);
    void handleListExperiments(const drogon::WebSocketConnectionPtr& conn,
                               const nlohmann::json& json);
    void handleSaveExperiment(const drogon::WebSocketConnectionPtr& conn,
                              const nlohmann::json& json);
    void handleDeleteExperiment(const drogon::WebSocketConnectionPtr& conn,
                                const nlohmann::json& json);

    // Recording mints an INSTANCE: an immutable snapshot of the board welded to
    // the data captured during that run (experiment-history-snapshots-plan,
    // Phases 2 + 3). start_ snapshots the live board and opens the window;
    // finish_ closes it and materializes the raw sources to Parquet.
    void handleStartExperimentInstance(const drogon::WebSocketConnectionPtr& conn,
                                       const nlohmann::json& json);
    void handleFinishExperimentInstance(const drogon::WebSocketConnectionPtr& conn,
                                        const nlohmann::json& json);
    // Drain each recorded source to Parquet + write the markers sidecar, verify it
    // captured something, checksum, seal. Runs on a detached thread (it takes tens
    // of seconds per stream) and broadcasts the outcome.
    void materializeInstance(const std::string& graph_id);

    // Replay (Phase 5): stream an instance's Parquet back onto a scratch Kafka
    // topic so the whole downstream graph works unchanged. Review mode is paced
    // from the original device_ts_us deltas; recompute mode is unpaced.
    void handleStartInstanceReplay(const drogon::WebSocketConnectionPtr& conn,
                                   const nlohmann::json& json);
    void handleStopInstanceReplay(const drogon::WebSocketConnectionPtr& conn,
                                  const nlohmann::json& json);
    void handleListInstanceReplays(const drogon::WebSocketConnectionPtr& conn,
                                   const nlohmann::json& json);

    // Re-check an instance's artifacts against their recorded sha256. A checksum
    // nobody verifies is decoration; this is what makes "detectable at review time"
    // true rather than aspirational.
    void handleVerifyExperimentInstance(const drogon::WebSocketConnectionPtr& conn,
                                        const nlohmann::json& json);

    // Delete a board, a fork, or (deliberately, with force) a sealed instance and
    // its artifacts.
    void handleDeleteStreamGraph(const drogon::WebSocketConnectionPtr& conn,
                                 const nlohmann::json& json);

    // Send a command to a device on its EXECUTION_COMMAND topic and wait for the
    // device's answer on its LOGGING_LOG topic, correlated by command_id. Runs the
    // produce-and-wait off the WS thread, so a device that never answers stalls
    // nothing but its own request.
    void handleSendDeviceCommand(const drogon::WebSocketConnectionPtr& conn,
                                 const nlohmann::json& json);

    // Start / stop pushing device_health snapshots to this client. The rig's
    // status topics are LOGGING_LOG, which the stream list deliberately does not
    // carry (they are not data a graph can consume), so a client cannot reach
    // them through subscribe -- this is the way in.
    void handleSubscribeDeviceHealth(const drogon::WebSocketConnectionPtr& conn,
                                     const nlohmann::json& json);
    void handleUnsubscribeDeviceHealth(const drogon::WebSocketConnectionPtr& conn);

    // Tails every NatKitNodeStatusV1 / NatKitPrimaryStatusV1 topic on the broker
    // and pushes a differenced snapshot once per interval.
    void deviceHealthThreadFunc(const drogon::WebSocketConnectionPtr& conn,
                                StreamViewerClientContext* ctx,
                                uint64_t interval_ms,
                                uint64_t quiet_after_ms);

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
    // requestId echoes the originating request so a client can tell WHICH call
    // failed. Errors used to be uncorrelatable, which matters as soon as a
    // rejection is expected in normal operation (e.g. saving an immutable
    // instance) rather than being a fatal surprise.
    void sendError(const drogon::WebSocketConnectionPtr& conn,
                   const std::string& message,
                   const std::string& requestId = std::string{});

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

    void sendProfileList(const drogon::WebSocketConnectionPtr& conn,
                         const std::string& request_id);
    void sendProfileSaved(const drogon::WebSocketConnectionPtr& conn,
                          const std::string& request_id,
                          const nlohmann::json& profile_json);
    void sendProfileDeleted(const drogon::WebSocketConnectionPtr& conn,
                            const std::string& request_id,
                            const std::string& participant_id);

    void sendExperimentList(const drogon::WebSocketConnectionPtr& conn,
                            const std::string& request_id);
    void sendExperimentInstance(const drogon::WebSocketConnectionPtr& conn,
                                const std::string& request_id,
                                const nlohmann::json& instance_json);
    void broadcastExperimentInstance(const nlohmann::json& instance_json);

    void sendInstanceReplayState(const drogon::WebSocketConnectionPtr& conn,
                                 const std::string& request_id,
                                 const std::string& replay_id,
                                 const std::string& graph_id,
                                 const natkit::tools::ReplayPlan& plan,
                                 const natkit::tools::ReplayProgress& progress,
                                 const std::string& state);
    void broadcastInstanceReplayState(const std::string& replay_id,
                                      const std::string& graph_id,
                                      const natkit::tools::ReplayPlan& plan,
                                      const natkit::tools::ReplayProgress& progress,
                                      const std::string& state);
    void sendExperimentSaved(const drogon::WebSocketConnectionPtr& conn,
                             const std::string& request_id,
                             const nlohmann::json& experiment_json);
    void sendExperimentDeleted(const drogon::WebSocketConnectionPtr& conn,
                               const std::string& request_id,
                               const std::string& experiment_id);

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
