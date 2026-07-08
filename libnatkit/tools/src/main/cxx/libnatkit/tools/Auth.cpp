#include "Auth.hpp"

#include <drogon/drogon.h>
#include <drogon/utils/Utilities.h>

#include <sqlite3.h>

#include <chrono>
#include <filesystem>
#include <iostream>
#include <random>
#include <regex>
#include <string_view>

using namespace drogon;

namespace {

uint64_t nowUs()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now().time_since_epoch())
            .count());
}

std::string trimCopy(const std::string& value)
{
    const auto begin = value.find_first_not_of(" \t\r\n");
    if (begin == std::string::npos) {
        return std::string{};
    }
    const auto end = value.find_last_not_of(" \t\r\n");
    return value.substr(begin, end - begin + 1);
}

bool parseEnvBool(const char* value, bool fallback)
{
    if (value == nullptr) {
        return fallback;
    }
    const std::string text = trimCopy(value);
    if (text == "1" || text == "true" || text == "TRUE" || text == "yes") {
        return true;
    }
    if (text == "0" || text == "false" || text == "FALSE" || text == "no") {
        return false;
    }
    return fallback;
}

void printBootstrapNotice(const std::string& message)
{
    std::cerr << message << std::endl;
    LOG_INFO << message;
}

class SqliteHandle {
public:
    explicit SqliteHandle(const std::string& path)
    {
        if (sqlite3_open(path.c_str(), &db_) != SQLITE_OK) {
            const std::string message =
                db_ != nullptr ? sqlite3_errmsg(db_) : "unknown sqlite error";
            if (db_ != nullptr) {
                sqlite3_close(db_);
                db_ = nullptr;
            }
            throw std::runtime_error("Failed to open auth database: " + message);
        }
    }

    ~SqliteHandle()
    {
        if (db_ != nullptr) {
            sqlite3_close(db_);
        }
    }

    sqlite3* get() const
    {
        return db_;
    }

private:
    sqlite3* db_ = nullptr;
};

class SqliteStatement {
public:
    SqliteStatement(sqlite3* db, const std::string& sql)
    {
        if (sqlite3_prepare_v2(db, sql.c_str(), -1, &stmt_, nullptr) != SQLITE_OK) {
            throw std::runtime_error(
                "Failed to prepare sqlite statement: " +
                std::string(sqlite3_errmsg(db)));
        }
    }

    ~SqliteStatement()
    {
        if (stmt_ != nullptr) {
            sqlite3_finalize(stmt_);
        }
    }

    sqlite3_stmt* get() const
    {
        return stmt_;
    }

private:
    sqlite3_stmt* stmt_ = nullptr;
};

std::string columnText(sqlite3_stmt* stmt, int column_index)
{
    const unsigned char* value = sqlite3_column_text(stmt, column_index);
    return value != nullptr ? reinterpret_cast<const char*>(value) : std::string{};
}

uint64_t columnUint64(sqlite3_stmt* stmt, int column_index)
{
    return static_cast<uint64_t>(sqlite3_column_int64(stmt, column_index));
}

bool columnBool(sqlite3_stmt* stmt, int column_index)
{
    return sqlite3_column_int(stmt, column_index) != 0;
}

void bindText(sqlite3_stmt* stmt, int index, const std::string& value)
{
    sqlite3_bind_text(stmt, index, value.c_str(), -1, SQLITE_TRANSIENT);
}

void bindInt64(sqlite3_stmt* stmt, int index, uint64_t value)
{
    sqlite3_bind_int64(stmt, index, static_cast<sqlite3_int64>(value));
}

void bindBool(sqlite3_stmt* stmt, int index, bool value)
{
    sqlite3_bind_int(stmt, index, value ? 1 : 0);
}

} // namespace

AuthManager& AuthManager::instance()
{
    static AuthManager instance;
    return instance;
}

AuthManager::AuthManager()
    : cookie_name_("natkit_session")
{
    const char* db_path_env = std::getenv("NATKIT_AUTH_DB_PATH");
    db_path_ = db_path_env != nullptr && std::string(db_path_env).size() > 0
        ? std::string(db_path_env)
        : std::string("uploads/auth.sqlite3");

    const char* cookie_name_env = std::getenv("NATKIT_AUTH_COOKIE_NAME");
    if (cookie_name_env != nullptr && std::string(cookie_name_env).size() > 0) {
        cookie_name_ = cookie_name_env;
    }

    const char* bootstrap_mode_env = std::getenv("NATKIT_AUTH_BOOTSTRAP_MODE");
    bootstrap_mode_ = bootstrap_mode_env != nullptr
            && std::string(bootstrap_mode_env) == "auto_admin"
        ? "auto_admin"
        : "password";

    const char* session_ttl_env = std::getenv("NATKIT_AUTH_SESSION_TTL_HOURS");
    if (session_ttl_env != nullptr) {
        try {
            const auto hours = std::stoull(session_ttl_env);
            if (hours > 0) {
                session_ttl_us_ = hours * 60ULL * 60ULL * 1000000ULL;
            }
        } catch (const std::exception&) {
        }
    }

    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();
    ensureBootstrapStateLocked();
}

bool AuthManager::bootstrapRequired() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return !anyUsersLocked();
}

std::string AuthManager::bootstrapMode() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    return bootstrap_mode_;
}

std::string AuthManager::sharedDbPath() const
{
    return db_path_;
}

std::string AuthManager::cookieName() const
{
    return cookie_name_;
}

std::optional<AuthenticatedUser> AuthManager::authenticateRequest(
    const HttpRequestPtr& req)
{
    const auto session_token = extractSessionToken(req);
    if (!session_token.has_value()) {
        return std::nullopt;
    }
    return authenticateSessionToken(session_token.value());
}

std::optional<AuthenticatedUser> AuthManager::authenticateSessionToken(
    const std::string& session_token)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();
    pruneExpiredSessionsLocked();

    const auto session = loadSessionLocked(session_token);
    if (!session.has_value()) {
        return std::nullopt;
    }

    const auto user = loadUserLocked(session->username);
    if (!user.has_value() || !user->enabled) {
        deleteSessionLocked(session_token);
        return std::nullopt;
    }

    StoredSession refreshed = session.value();
    refreshed.expires_at_us = nowUs() + session_ttl_us_;
    saveSessionLocked(refreshed);
    return toAuthenticatedUser(user.value());
}

AuthResult AuthManager::bootstrapAdmin(
    const std::string& username,
    const std::string& display_name,
    const std::string& password,
    const std::string& bootstrap_password)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    if (anyUsersLocked()) {
        return AuthResult{false, k409Conflict, "Bootstrap is already complete."};
    }
    if (!validateUsername(username)) {
        return AuthResult{false, k400BadRequest, "Username must be 3-32 characters of letters, digits, ., _, or -."};
    }
    if (!validatePassword(password)) {
        return AuthResult{false, k400BadRequest, "Password must be at least 10 characters."};
    }
    if (bootstrap_mode_ == "password" && bootstrap_password != bootstrap_password_) {
        return AuthResult{false, k403Forbidden, "Bootstrap password is invalid."};
    }

    const uint64_t current_us = nowUs();
    StoredUser user{};
    user.username = username;
    user.display_name = display_name.empty() ? username : display_name;
    user.password_salt = createRandomToken(16);
    user.password_hash = hashPassword(user.password_salt, password);
    user.is_admin = true;
    user.enabled = true;
    user.shared_compute_access = true;
    user.created_at_us = current_us;
    user.updated_at_us = current_us;
    user.password_updated_at_us = current_us;
    saveUserLocked(user);

    bootstrap_password_.clear();
    return buildSessionForUserLocked(user);
}

AuthResult AuthManager::login(const std::string& username, const std::string& password)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    if (!anyUsersLocked()) {
        ensureBootstrapStateLocked();
        return AuthResult{false, k409Conflict, "Bootstrap is required before login."};
    }

    const auto user = loadUserLocked(username);
    if (!user.has_value() || !user->enabled) {
        return AuthResult{false, k401Unauthorized, "Invalid username or password."};
    }
    if (!passwordsMatch(user->password_hash, hashPassword(user->password_salt, password))) {
        return AuthResult{false, k401Unauthorized, "Invalid username or password."};
    }

    return buildSessionForUserLocked(user.value());
}

void AuthManager::logout(const HttpRequestPtr& req, HttpResponsePtr& resp)
{
    std::lock_guard<std::mutex> lock(mutex_);
    const auto session_token = extractSessionToken(req);
    if (session_token.has_value()) {
        deleteSessionLocked(session_token.value());
    }
    clearSessionCookie(resp);
}

std::vector<AuthUserSummary> AuthManager::listUsers() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    std::vector<AuthUserSummary> users;
    for (const auto& user : loadAllUsersLocked()) {
        users.push_back(toUserSummary(user));
    }
    return users;
}

AuthResult AuthManager::createUser(
    const std::string& actor_username,
    const std::string& username,
    const std::string& display_name,
    const std::string& password,
    bool is_admin,
    bool enabled,
    bool shared_compute_access)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    const auto actor = loadUserLocked(actor_username);
    if (!actor.has_value() || !actor->is_admin || !actor->enabled) {
        return AuthResult{false, k403Forbidden, "Actor is not authorized."};
    }
    if (!validateUsername(username)) {
        return AuthResult{false, k400BadRequest, "Username must be 3-32 characters of letters, digits, ., _, or -."};
    }
    if (!validatePassword(password)) {
        return AuthResult{false, k400BadRequest, "Password must be at least 10 characters."};
    }
    if (loadUserLocked(username).has_value()) {
        return AuthResult{false, k409Conflict, "User already exists."};
    }

    const uint64_t current_us = nowUs();
    StoredUser user{};
    user.username = username;
    user.display_name = display_name.empty() ? username : display_name;
    user.password_salt = createRandomToken(16);
    user.password_hash = hashPassword(user.password_salt, password);
    user.is_admin = is_admin;
    user.enabled = enabled;
    user.shared_compute_access = shared_compute_access;
    user.created_at_us = current_us;
    user.updated_at_us = current_us;
    user.password_updated_at_us = current_us;
    saveUserLocked(user);

    AuthResult result{};
    result.ok = true;
    result.status = k200OK;
    result.message = "User created.";
    result.user = toAuthenticatedUser(user);
    return result;
}

AuthResult AuthManager::updateUser(
    const std::string& actor_username,
    const std::string& username,
    const std::optional<std::string>& display_name,
    const std::optional<std::string>& password,
    const std::optional<bool>& is_admin,
    const std::optional<bool>& enabled,
    const std::optional<bool>& shared_compute_access)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    const auto actor = loadUserLocked(actor_username);
    if (!actor.has_value() || !actor->is_admin || !actor->enabled) {
        return AuthResult{false, k403Forbidden, "Actor is not authorized."};
    }

    auto user = loadUserLocked(username);
    if (!user.has_value()) {
        return AuthResult{false, k404NotFound, "User not found."};
    }
    if (password.has_value() && !validatePassword(password.value())) {
        return AuthResult{false, k400BadRequest, "Password must be at least 10 characters."};
    }

    const bool next_is_admin = is_admin.has_value() ? is_admin.value() : user->is_admin;
    const bool next_enabled = enabled.has_value() ? enabled.value() : user->enabled;
    if ((!next_is_admin || !next_enabled) && user->is_admin && user->enabled &&
        !enabledAdminExistsOtherThanLocked(user->username)) {
        return AuthResult{false, k400BadRequest, "At least one enabled admin must remain."};
    }

    if (display_name.has_value()) {
        user->display_name = display_name->empty() ? username : display_name.value();
    }
    if (password.has_value()) {
        user->password_salt = createRandomToken(16);
        user->password_hash = hashPassword(user->password_salt, password.value());
        user->password_updated_at_us = nowUs();
    }
    user->is_admin = next_is_admin;
    user->enabled = next_enabled;
    if (shared_compute_access.has_value()) {
        user->shared_compute_access = shared_compute_access.value();
    }
    user->updated_at_us = nowUs();
    saveUserLocked(user.value());

    if (!user->enabled) {
        deleteSessionsForUserLocked(user->username);
    }

    AuthResult result{};
    result.ok = true;
    result.status = k200OK;
    result.message = "User updated.";
    result.user = toAuthenticatedUser(user.value());
    return result;
}

std::vector<ComputeSlotPolicy> AuthManager::listComputeSlotPolicies() const
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();
    return loadAllComputeSlotPoliciesLocked();
}

AuthResult AuthManager::setComputeSlotPolicy(
    const std::string& actor_username,
    const std::string& slot_id,
    const std::string& access_mode,
    const std::optional<std::string>& dedicated_username)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    const auto actor = loadUserLocked(actor_username);
    if (!actor.has_value() || !actor->is_admin || !actor->enabled) {
        return AuthResult{false, k403Forbidden, "Actor is not authorized."};
    }

    const std::string normalized_slot_id = trimCopy(slot_id);
    if (normalized_slot_id.empty()) {
        return AuthResult{false, k400BadRequest, "slot_id is required."};
    }

    const std::string normalized_access_mode = trimCopy(access_mode);
    if (normalized_access_mode != "shared" && normalized_access_mode != "dedicated") {
        return AuthResult{false, k400BadRequest, "access_mode must be shared or dedicated."};
    }

    std::string normalized_dedicated_username;
    if (dedicated_username.has_value()) {
        normalized_dedicated_username = trimCopy(dedicated_username.value());
    }
    if (normalized_access_mode == "dedicated") {
        if (normalized_dedicated_username.empty()) {
            return AuthResult{false, k400BadRequest, "dedicated_username is required for dedicated slots."};
        }
        const auto dedicated_user = loadUserLocked(normalized_dedicated_username);
        if (!dedicated_user.has_value() || !dedicated_user->enabled) {
            return AuthResult{false, k404NotFound, "Dedicated user not found or disabled."};
        }
    } else {
        normalized_dedicated_username.clear();
    }

    ComputeSlotPolicy policy{};
    policy.slot_id = normalized_slot_id;
    policy.access_mode = normalized_access_mode;
    policy.dedicated_username = normalized_dedicated_username;
    policy.updated_at_us = nowUs();
    saveComputeSlotPolicyLocked(policy);

    AuthResult result{};
    result.ok = true;
    result.status = k200OK;
    result.message = "Compute slot policy updated.";
    return result;
}

AuthResult AuthManager::deleteUser(
    const std::string& actor_username,
    const std::string& username)
{
    std::lock_guard<std::mutex> lock(mutex_);
    initializeDatabaseLocked();

    const auto actor = loadUserLocked(actor_username);
    if (!actor.has_value() || !actor->is_admin || !actor->enabled) {
        return AuthResult{false, k403Forbidden, "Actor is not authorized."};
    }
    if (actor_username == username) {
        return AuthResult{false, k400BadRequest, "Admins cannot delete their own account from this panel."};
    }

    const auto user = loadUserLocked(username);
    if (!user.has_value()) {
        return AuthResult{false, k404NotFound, "User not found."};
    }
    if (user->is_admin && user->enabled && !enabledAdminExistsOtherThanLocked(username)) {
        return AuthResult{false, k400BadRequest, "At least one enabled admin must remain."};
    }

    deleteUserLocked(username);
    deleteSessionsForUserLocked(username);

    AuthResult result{};
    result.ok = true;
    result.status = k200OK;
    result.message = "User deleted.";
    return result;
}

void AuthManager::applySessionCookie(
    const std::optional<std::string>& session_token,
    HttpResponsePtr& resp) const
{
    if (!session_token.has_value()) {
        return;
    }

    Cookie cookie(cookie_name_, session_token.value());
    cookie.setHttpOnly(true);
    cookie.setPath("/");
    cookie.setSameSite(Cookie::SameSite::kLax);
    cookie.setSecure(parseEnvBool(std::getenv("NATKIT_AUTH_SECURE_COOKIE"), false));
    cookie.setMaxAge(static_cast<int>(session_ttl_us_ / 1000000ULL));
    resp->addCookie(std::move(cookie));
}

void AuthManager::clearSessionCookie(HttpResponsePtr& resp) const
{
    Cookie cookie(cookie_name_, "");
    cookie.setHttpOnly(true);
    cookie.setPath("/");
    cookie.setSameSite(Cookie::SameSite::kLax);
    cookie.setSecure(parseEnvBool(std::getenv("NATKIT_AUTH_SECURE_COOKIE"), false));
    cookie.setMaxAge(0);
    resp->addCookie(std::move(cookie));
}

std::optional<std::string> AuthManager::extractSessionToken(
    const HttpRequestPtr& req) const
{
    const auto& token = req->getCookie(cookie_name_);
    if (!token.empty()) {
        return token;
    }
    return std::nullopt;
}

AuthenticatedUser AuthManager::toAuthenticatedUser(const StoredUser& user) const
{
    return AuthenticatedUser{user.username, user.display_name, user.is_admin};
}

AuthUserSummary AuthManager::toUserSummary(const StoredUser& user) const
{
    return AuthUserSummary{
        user.username,
        user.display_name,
        user.is_admin,
        user.enabled,
        user.shared_compute_access,
        user.created_at_us,
        user.updated_at_us,
        user.password_updated_at_us,
        user.last_login_at_us,
    };
}

void AuthManager::initializeDatabaseLocked() const
{
    std::filesystem::path output_path(db_path_);
    if (output_path.has_parent_path()) {
        std::filesystem::create_directories(output_path.parent_path());
    }

    SqliteHandle db(db_path_);
    const char* sql = R"SQL(
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS auth_users (
            username TEXT PRIMARY KEY,
            display_name TEXT NOT NULL,
            password_salt TEXT NOT NULL,
            password_hash TEXT NOT NULL,
            is_admin INTEGER NOT NULL,
            enabled INTEGER NOT NULL,
            shared_compute_access INTEGER NOT NULL DEFAULT 0,
            created_at_us INTEGER NOT NULL,
            updated_at_us INTEGER NOT NULL,
            password_updated_at_us INTEGER NOT NULL,
            last_login_at_us INTEGER NOT NULL
        );
        CREATE TABLE IF NOT EXISTS auth_sessions (
            token TEXT PRIMARY KEY,
            username TEXT NOT NULL,
            created_at_us INTEGER NOT NULL,
            expires_at_us INTEGER NOT NULL,
            FOREIGN KEY(username) REFERENCES auth_users(username) ON DELETE CASCADE
        );
        CREATE INDEX IF NOT EXISTS idx_auth_sessions_username ON auth_sessions(username);
        CREATE INDEX IF NOT EXISTS idx_auth_sessions_expires_at ON auth_sessions(expires_at_us);
        CREATE TABLE IF NOT EXISTS compute_slot_policies (
            slot_id TEXT PRIMARY KEY,
            access_mode TEXT NOT NULL,
            dedicated_username TEXT,
            updated_at_us INTEGER NOT NULL,
            FOREIGN KEY(dedicated_username) REFERENCES auth_users(username) ON DELETE SET NULL
        );
    )SQL";

    char* error_message = nullptr;
    if (sqlite3_exec(db.get(), sql, nullptr, nullptr, &error_message) != SQLITE_OK) {
        const std::string message =
            error_message != nullptr ? error_message : "unknown sqlite error";
        sqlite3_free(error_message);
        throw std::runtime_error("Failed to initialize auth database: " + message);
    }

    bool has_shared_compute_access = false;
    {
        SqliteStatement stmt(db.get(), "PRAGMA table_info(auth_users)");
        while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
            if (columnText(stmt.get(), 1) == "shared_compute_access") {
                has_shared_compute_access = true;
                break;
            }
        }
    }
    if (!has_shared_compute_access) {
        if (sqlite3_exec(
                db.get(),
                "ALTER TABLE auth_users ADD COLUMN shared_compute_access INTEGER NOT NULL DEFAULT 0",
                nullptr,
                nullptr,
                &error_message) != SQLITE_OK) {
            const std::string message =
                error_message != nullptr ? error_message : "unknown sqlite error";
            sqlite3_free(error_message);
            throw std::runtime_error("Failed to migrate auth_users: " + message);
        }
    }
}

bool AuthManager::anyUsersLocked() const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(db.get(), "SELECT COUNT(*) FROM auth_users");
    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        throw std::runtime_error("Failed to count auth users.");
    }
    return sqlite3_column_int64(stmt.get(), 0) > 0;
}

void AuthManager::pruneExpiredSessionsLocked() const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "DELETE FROM auth_sessions WHERE expires_at_us <= ?");
    bindInt64(stmt.get(), 1, nowUs());
    sqlite3_step(stmt.get());
}

void AuthManager::ensureBootstrapStateLocked()
{
    if (anyUsersLocked()) {
        bootstrap_password_.clear();
        return;
    }

    if (bootstrap_mode_ == "password") {
        const char* configured = std::getenv("NATKIT_AUTH_BOOTSTRAP_PASSWORD");
        bootstrap_password_ =
            configured != nullptr && std::string(configured).size() > 0
                ? std::string(configured)
                : createRandomToken(12);
        printBootstrapNotice(
            "Auth bootstrap required. Initial admin bootstrap password: "
            + bootstrap_password_);
    } else {
        printBootstrapNotice(
            "Auth bootstrap required. Mode=auto_admin. First bootstrap request "
            "will create the initial admin.");
    }
}

std::optional<AuthManager::StoredUser> AuthManager::loadUserLocked(
    const std::string& username) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "SELECT username, display_name, password_salt, password_hash, is_admin, enabled,"
        " shared_compute_access, created_at_us, updated_at_us, password_updated_at_us, last_login_at_us"
        " FROM auth_users WHERE username = ?");
    bindText(stmt.get(), 1, username);
    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        return std::nullopt;
    }

    StoredUser user{};
    user.username = columnText(stmt.get(), 0);
    user.display_name = columnText(stmt.get(), 1);
    user.password_salt = columnText(stmt.get(), 2);
    user.password_hash = columnText(stmt.get(), 3);
    user.is_admin = columnBool(stmt.get(), 4);
    user.enabled = columnBool(stmt.get(), 5);
    user.shared_compute_access = columnBool(stmt.get(), 6);
    user.created_at_us = columnUint64(stmt.get(), 7);
    user.updated_at_us = columnUint64(stmt.get(), 8);
    user.password_updated_at_us = columnUint64(stmt.get(), 9);
    user.last_login_at_us = columnUint64(stmt.get(), 10);
    return user;
}

std::optional<AuthManager::StoredSession> AuthManager::loadSessionLocked(
    const std::string& token) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "SELECT token, username, created_at_us, expires_at_us"
        " FROM auth_sessions WHERE token = ?");
    bindText(stmt.get(), 1, token);
    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        return std::nullopt;
    }

    StoredSession session{};
    session.token = columnText(stmt.get(), 0);
    session.username = columnText(stmt.get(), 1);
    session.created_at_us = columnUint64(stmt.get(), 2);
    session.expires_at_us = columnUint64(stmt.get(), 3);
    return session;
}

std::vector<AuthManager::StoredUser> AuthManager::loadAllUsersLocked() const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "SELECT username, display_name, password_salt, password_hash, is_admin, enabled,"
        " shared_compute_access, created_at_us, updated_at_us, password_updated_at_us, last_login_at_us"
        " FROM auth_users ORDER BY username ASC");

    std::vector<StoredUser> users;
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        StoredUser user{};
        user.username = columnText(stmt.get(), 0);
        user.display_name = columnText(stmt.get(), 1);
        user.password_salt = columnText(stmt.get(), 2);
        user.password_hash = columnText(stmt.get(), 3);
        user.is_admin = columnBool(stmt.get(), 4);
        user.enabled = columnBool(stmt.get(), 5);
        user.shared_compute_access = columnBool(stmt.get(), 6);
        user.created_at_us = columnUint64(stmt.get(), 7);
        user.updated_at_us = columnUint64(stmt.get(), 8);
        user.password_updated_at_us = columnUint64(stmt.get(), 9);
        user.last_login_at_us = columnUint64(stmt.get(), 10);
        users.push_back(user);
    }
    return users;
}

std::vector<ComputeSlotPolicy> AuthManager::loadAllComputeSlotPoliciesLocked() const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "SELECT slot_id, access_mode, dedicated_username, updated_at_us"
        " FROM compute_slot_policies ORDER BY slot_id ASC");

    std::vector<ComputeSlotPolicy> policies;
    while (sqlite3_step(stmt.get()) == SQLITE_ROW) {
        ComputeSlotPolicy policy{};
        policy.slot_id = columnText(stmt.get(), 0);
        policy.access_mode = columnText(stmt.get(), 1);
        policy.dedicated_username = columnText(stmt.get(), 2);
        policy.updated_at_us = columnUint64(stmt.get(), 3);
        policies.push_back(policy);
    }
    return policies;
}

void AuthManager::saveUserLocked(const StoredUser& user) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "INSERT INTO auth_users (username, display_name, password_salt, password_hash, is_admin,"
        " enabled, shared_compute_access, created_at_us, updated_at_us, password_updated_at_us, last_login_at_us)"
        " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        " ON CONFLICT(username) DO UPDATE SET"
        " display_name=excluded.display_name,"
        " password_salt=excluded.password_salt,"
        " password_hash=excluded.password_hash,"
        " is_admin=excluded.is_admin,"
        " enabled=excluded.enabled,"
        " shared_compute_access=excluded.shared_compute_access,"
        " created_at_us=excluded.created_at_us,"
        " updated_at_us=excluded.updated_at_us,"
        " password_updated_at_us=excluded.password_updated_at_us,"
        " last_login_at_us=excluded.last_login_at_us");
    bindText(stmt.get(), 1, user.username);
    bindText(stmt.get(), 2, user.display_name);
    bindText(stmt.get(), 3, user.password_salt);
    bindText(stmt.get(), 4, user.password_hash);
    bindBool(stmt.get(), 5, user.is_admin);
    bindBool(stmt.get(), 6, user.enabled);
    bindBool(stmt.get(), 7, user.shared_compute_access);
    bindInt64(stmt.get(), 8, user.created_at_us);
    bindInt64(stmt.get(), 9, user.updated_at_us);
    bindInt64(stmt.get(), 10, user.password_updated_at_us);
    bindInt64(stmt.get(), 11, user.last_login_at_us);
    sqlite3_step(stmt.get());
}

void AuthManager::saveSessionLocked(const StoredSession& session) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "INSERT INTO auth_sessions (token, username, created_at_us, expires_at_us)"
        " VALUES (?, ?, ?, ?)"
        " ON CONFLICT(token) DO UPDATE SET"
        " username=excluded.username,"
        " created_at_us=excluded.created_at_us,"
        " expires_at_us=excluded.expires_at_us");
    bindText(stmt.get(), 1, session.token);
    bindText(stmt.get(), 2, session.username);
    bindInt64(stmt.get(), 3, session.created_at_us);
    bindInt64(stmt.get(), 4, session.expires_at_us);
    sqlite3_step(stmt.get());
}

void AuthManager::saveComputeSlotPolicyLocked(const ComputeSlotPolicy& policy) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "INSERT INTO compute_slot_policies (slot_id, access_mode, dedicated_username, updated_at_us)"
        " VALUES (?, ?, ?, ?)"
        " ON CONFLICT(slot_id) DO UPDATE SET"
        " access_mode=excluded.access_mode,"
        " dedicated_username=excluded.dedicated_username,"
        " updated_at_us=excluded.updated_at_us");
    bindText(stmt.get(), 1, policy.slot_id);
    bindText(stmt.get(), 2, policy.access_mode);
    if (policy.dedicated_username.empty()) {
        sqlite3_bind_null(stmt.get(), 3);
    } else {
        bindText(stmt.get(), 3, policy.dedicated_username);
    }
    bindInt64(stmt.get(), 4, policy.updated_at_us);
    sqlite3_step(stmt.get());
}

void AuthManager::deleteUserLocked(const std::string& username) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "DELETE FROM auth_users WHERE username = ?");
    bindText(stmt.get(), 1, username);
    sqlite3_step(stmt.get());
}

void AuthManager::deleteSessionsForUserLocked(const std::string& username) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "DELETE FROM auth_sessions WHERE username = ?");
    bindText(stmt.get(), 1, username);
    sqlite3_step(stmt.get());
}

void AuthManager::deleteSessionLocked(const std::string& token) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "DELETE FROM auth_sessions WHERE token = ?");
    bindText(stmt.get(), 1, token);
    sqlite3_step(stmt.get());
}

bool AuthManager::enabledAdminExistsOtherThanLocked(const std::string& username) const
{
    SqliteHandle db(db_path_);
    SqliteStatement stmt(
        db.get(),
        "SELECT COUNT(*) FROM auth_users"
        " WHERE username != ? AND is_admin = 1 AND enabled = 1");
    bindText(stmt.get(), 1, username);
    if (sqlite3_step(stmt.get()) != SQLITE_ROW) {
        return false;
    }
    return sqlite3_column_int64(stmt.get(), 0) > 0;
}

bool AuthManager::validateUsername(const std::string& username) const
{
    static const std::regex pattern("^[A-Za-z0-9._-]{3,32}$");
    return std::regex_match(username, pattern);
}

bool AuthManager::validatePassword(const std::string& password) const
{
    return password.size() >= 10;
}

std::string AuthManager::createRandomToken(size_t byte_count) const
{
    std::random_device rd;
    std::uniform_int_distribution<int> dist(0, 255);
    std::string bytes;
    bytes.resize(byte_count);
    for (size_t i = 0; i < byte_count; ++i) {
        bytes[i] = static_cast<char>(dist(rd));
    }
    return drogon::utils::base64EncodeUnpadded(
        reinterpret_cast<const unsigned char*>(bytes.data()),
        bytes.size(),
        true);
}

std::string AuthManager::hashPassword(
    const std::string& salt,
    const std::string& password) const
{
    std::string digest = salt + ":" + password;
    for (size_t round = 0; round < 100000; ++round) {
        digest = drogon::utils::getSha256(digest);
    }
    return digest;
}

bool AuthManager::passwordsMatch(
    const std::string& lhs,
    const std::string& rhs) const
{
    if (lhs.size() != rhs.size()) {
        return false;
    }
    unsigned char diff = 0;
    for (size_t i = 0; i < lhs.size(); ++i) {
        diff |= static_cast<unsigned char>(lhs[i] ^ rhs[i]);
    }
    return diff == 0;
}

AuthResult AuthManager::buildSessionForUserLocked(StoredUser user)
{
    pruneExpiredSessionsLocked();

    const uint64_t current_us = nowUs();
    user.last_login_at_us = current_us;
    user.updated_at_us = current_us;
    saveUserLocked(user);

    StoredSession session{};
    session.token = createRandomToken(32);
    session.username = user.username;
    session.created_at_us = current_us;
    session.expires_at_us = current_us + session_ttl_us_;
    saveSessionLocked(session);

    AuthResult result{};
    result.ok = true;
    result.status = k200OK;
    result.message = "Authenticated.";
    result.user = toAuthenticatedUser(user);
    result.session_token = session.token;
    return result;
}
