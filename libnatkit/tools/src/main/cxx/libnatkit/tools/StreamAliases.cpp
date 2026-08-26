#include "StreamAliases.hpp"

#include <chrono>
#include <mutex>
#include <sqlite3.h>
#include <stdexcept>

#include "Auth.hpp"

namespace nat {
namespace tools {
namespace {

// Small RAII pair, deliberately local rather than shared with Auth.cpp: those
// live in an anonymous namespace there, and exporting them to make this file two
// lines shorter would widen an internal detail into an interface.
class Db {
public:
    explicit Db(const std::string& path)
    {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
            const std::string message =
                db_ != nullptr ? sqlite3_errmsg(db_) : "unknown sqlite error";
            if (db_ != nullptr) sqlite3_close(db_);
            throw std::runtime_error("Failed to open the natKit database: " + message);
        }
        sqlite3_busy_timeout(db_, 3000);
    }
    ~Db() { if (db_ != nullptr) sqlite3_close(db_); }
    Db(const Db&) = delete;
    Db& operator=(const Db&) = delete;
    sqlite3* get() const { return db_; }

private:
    sqlite3* db_ = nullptr;
};

class Stmt {
public:
    Stmt(sqlite3* db, const std::string& sql)
    {
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error("Failed to prepare statement: " +
                                     std::string(sqlite3_errmsg(db)));
        }
    }
    ~Stmt() { if (stmt_ != nullptr) sqlite3_finalize(stmt_); }
    Stmt(const Stmt&) = delete;
    Stmt& operator=(const Stmt&) = delete;
    sqlite3_stmt* get() const { return stmt_; }

private:
    sqlite3_stmt* stmt_ = nullptr;
};

std::string textAt(sqlite3_stmt* stmt, int column)
{
    const auto* value = sqlite3_column_text(stmt, column);
    return value == nullptr ? std::string{}
                            : std::string(reinterpret_cast<const char*>(value));
}

uint64_t nowUs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::mutex& schemaMutex()
{
    static std::mutex mutex;
    return mutex;
}

}  // namespace

StreamAliasStore& StreamAliasStore::instance()
{
    static StreamAliasStore store;
    return store;
}

std::string StreamAliasStore::dbPath() const
{
    return AuthManager::instance().sharedDbPath();
}

void StreamAliasStore::ensureSchema() const
{
    static bool created = false;
    std::lock_guard<std::mutex> lock(schemaMutex());
    if (created) return;

    Db db(dbPath());
    const char* sql = R"SQL(
        CREATE TABLE IF NOT EXISTS stream_aliases (
            stream_id TEXT NOT NULL,
            alias TEXT NOT NULL,
            owner_username TEXT,
            created_at_us INTEGER NOT NULL,
            updated_at_us INTEGER NOT NULL,
            FOREIGN KEY(owner_username) REFERENCES auth_users(username) ON DELETE CASCADE
        );
    )SQL";
    char* error = nullptr;
    if (sqlite3_exec(db.get(), sql, nullptr, nullptr, &error) != SQLITE_OK) {
        const std::string message = error != nullptr ? error : "unknown sqlite error";
        sqlite3_free(error);
        throw std::runtime_error("Failed to create stream_aliases: " + message);
    }

    // ⚠️ IFNULL, AND IT IS LOAD-BEARING. In SQLite two NULLs are DISTINCT in a
    // unique index, so `UNIQUE(stream_id, owner_username)` would happily accept
    // the same global alias twice — and `visibleTo` would then return whichever
    // row the query planner reached first, so a rename would appear to work and
    // then silently revert. Folding NULL to '' makes one global row per stream
    // the thing the database enforces rather than something the writer has to
    // remember.
    const char* index_sql =
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_stream_aliases_owner "
        "ON stream_aliases(stream_id, IFNULL(owner_username, ''))";
    if (sqlite3_exec(db.get(), index_sql, nullptr, nullptr, &error) != SQLITE_OK) {
        const std::string message = error != nullptr ? error : "unknown sqlite error";
        sqlite3_free(error);
        throw std::runtime_error("Failed to index stream_aliases: " + message);
    }
    created = true;
}

std::map<std::string, std::string> StreamAliasStore::visibleTo(
    const std::string& username) const
{
    ensureSchema();
    Db db(dbPath());
    // ⚠️ Ordered so a row OWNED by this user overwrites the global one as the map
    // is filled: globals first, the user's own second. Today no row has an owner,
    // so this is a no-op — but writing it now means enabling per-user aliases does
    // not also require getting the precedence right under time pressure.
    Stmt stmt(db.get(),
              "SELECT stream_id, alias FROM stream_aliases "
              "WHERE owner_username IS NULL OR owner_username = ?1 "
              "ORDER BY (owner_username IS NOT NULL) ASC");
    sqlite3_bind_text(stmt.get(), 1, username.c_str(), -1, SQLITE_TRANSIENT);

    std::map<std::string, std::string> out;
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        out[textAt(stmt.get(), 0)] = textAt(stmt.get(), 1);
    }
    return out;
}

void StreamAliasStore::set(const std::string& streamId,
                           const std::string& alias,
                           const std::string& ownerUsername) const
{
    if (streamId.empty()) return;
    ensureSchema();
    Db db(dbPath());

    // ⚠️ AN EMPTY ALIAS DELETES. Storing "" would make "named with nothing" and
    // "never named" two states that render identically and compare differently,
    // and the fallback chain would have to test for both forever.
    if (alias.empty()) {
        Stmt stmt(db.get(),
                  "DELETE FROM stream_aliases WHERE stream_id = ?1 "
                  "AND IFNULL(owner_username, '') = ?2");
        sqlite3_bind_text(stmt.get(), 1, streamId.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt.get(), 2, ownerUsername.c_str(), -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt.get());
        return;
    }

    const auto now = nowUs();
    Stmt stmt(db.get(),
              "INSERT INTO stream_aliases "
              "  (stream_id, alias, owner_username, created_at_us, updated_at_us) "
              "VALUES (?1, ?2, NULLIF(?3, ''), ?4, ?4) "
              "ON CONFLICT(stream_id, IFNULL(owner_username, '')) DO UPDATE SET "
              "  alias = excluded.alias, updated_at_us = excluded.updated_at_us");
    sqlite3_bind_text(stmt.get(), 1, streamId.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 2, alias.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt.get(), 3, ownerUsername.c_str(), -1, SQLITE_TRANSIENT);
    sqlite3_bind_int64(stmt.get(), 4, static_cast<sqlite3_int64>(now));
    if (sqlite3_step(stmt.get()) != SQLITE_DONE) {
        throw std::runtime_error("Failed to store stream alias: " +
                                 std::string(sqlite3_errmsg(db.get())));
    }
}

std::vector<StreamAlias> StreamAliasStore::all() const
{
    ensureSchema();
    Db db(dbPath());
    Stmt stmt(db.get(),
              "SELECT stream_id, alias, IFNULL(owner_username, ''), updated_at_us "
              "FROM stream_aliases ORDER BY stream_id");
    std::vector<StreamAlias> out;
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        StreamAlias row;
        row.streamId = textAt(stmt.get(), 0);
        row.alias = textAt(stmt.get(), 1);
        row.ownerUsername = textAt(stmt.get(), 2);
        row.updatedAtUs =
            static_cast<uint64_t>(sqlite3_column_int64(stmt.get(), 3));
        out.push_back(std::move(row));
    }
    return out;
}

}  // namespace tools
}  // namespace nat
