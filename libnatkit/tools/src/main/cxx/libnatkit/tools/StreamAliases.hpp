#pragma once

// Friendly names for streams (TEC-NATKIT-103).
//
// A stream is identified on the wire by a number — 13793649671244 — and four
// identical boards on a bench differ by two digits in the middle. An alias is
// the human's name for one: "Left Hand". It is presented as
// `alias (13793649671244)` everywhere, never instead of the id, because the id
// is what every topic, recording and log line is keyed by and hiding it would
// make those unsearchable.
//
// --- Why this lives in the auth database ------------------------------------
//
// ⚠️ NOT because an alias has anything to do with authentication. Because it
// will one day BELONG TO A USER, and a foreign key to `auth_users` has to live
// in the same SQLite file. The alternative — a separate database — cannot
// reference a user at all, so going multi-user would mean moving the table
// anyway. That file is really the natKit application database that happened to
// start with auth in it.
//
// It is also already on a durable named volume in all three compose files,
// which a fifth JSON store would not have been. Three separate outages came
// from a store defaulting to the container layer (TEC-NATKIT-66, -414, -415).
//
// --- Global today, per-user tomorrow ----------------------------------------
//
// ⚠️ `owner_username` EXISTS NOW and is NULL for every row. NULL means "global:
// everyone sees this name". Adding multi-user support is then a behaviour
// change — start writing the current user into that column — rather than a
// schema migration against live data. The same shape `compute_slot_policies`
// already uses for `dedicated_username`.

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace nat {
namespace tools {

struct StreamAlias {
    std::string streamId;
    std::string alias;
    /** Empty means the global alias. */
    std::string ownerUsername;
    uint64_t updatedAtUs = 0;
};

class StreamAliasStore {
public:
    static StreamAliasStore& instance();

    /** Creates the table if absent. Idempotent; safe to call per request. */
    void ensureSchema() const;

    /**
     * Every alias visible to `username`, keyed by stream id.
     *
     * Today every row is global, so this returns the same map for everyone. When
     * rows start carrying an owner, a user's own alias should win over the
     * global one — which is why this takes the username now rather than growing
     * a parameter later.
     */
    std::map<std::string, std::string> visibleTo(
        const std::string& username = std::string{}) const;

    /**
     * Set or clear an alias. An empty `alias` DELETES the row rather than
     * storing an empty string: "" and "no alias" would otherwise be two states
     * that render identically and compare differently.
     */
    void set(const std::string& streamId,
             const std::string& alias,
             const std::string& ownerUsername = std::string{}) const;

    std::vector<StreamAlias> all() const;

private:
    StreamAliasStore() = default;
    std::string dbPath() const;
};

/**
 * Which streams the IMU surfaces watch (TEC-NATKIT-98 follow-up).
 *
 * ⚠️ This selection used to live ONLY in a member of the API controller, so
 * every backend restart silently emptied it. Nothing said so: the streams kept
 * arriving, the boards kept their wiring, and the calibration readout on a
 * correctly-wired node just reported "No IMU streams selected" — a message that
 * points at a settings page rather than at the restart that actually caused it.
 * Rebuilding the backend is routine here, so this was lost several times a day.
 *
 * Same file as the aliases deliberately: both are small per-stream settings that
 * have to outlive a restart, and a second copy of the SQLite RAII helpers in a
 * third translation unit would be worse than the slightly broad file name. Same
 * database, and the same NULL-owner-means-global shape.
 */
class ImuStreamSelectionStore {
public:
    static ImuStreamSelectionStore& instance();

    /** Creates the table if absent. Idempotent; safe to call per request. */
    void ensureSchema() const;

    /** The selected stream ids, empty when nothing has ever been chosen. */
    std::vector<uint64_t> selected(const std::string& username = std::string{}) const;

    /**
     * Replaces the whole selection — it is a set, not a list of edits, and the
     * UI always sends the complete set.
     */
    void replace(const std::vector<uint64_t>& streamIds,
                 const std::string& ownerUsername = std::string{}) const;

private:
    ImuStreamSelectionStore() = default;
    std::string dbPath() const;
};

}  // namespace tools
}  // namespace nat
