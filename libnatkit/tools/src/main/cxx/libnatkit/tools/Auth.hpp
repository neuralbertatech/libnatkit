#pragma once

#include <drogon/Cookie.h>
#include <drogon/HttpRequest.h>
#include <drogon/HttpResponse.h>
#include <drogon/HttpTypes.h>

#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

struct AuthenticatedUser {
    std::string username;
    std::string display_name;
    bool is_admin = false;
};

struct AuthResult {
    bool ok = false;
    drogon::HttpStatusCode status = drogon::k400BadRequest;
    std::string message;
    std::optional<AuthenticatedUser> user;
    std::optional<std::string> session_token;
};

struct AuthUserSummary {
    std::string username;
    std::string display_name;
    bool is_admin = false;
    bool enabled = true;
    bool shared_compute_access = false;
    uint64_t created_at_us = 0;
    uint64_t updated_at_us = 0;
    uint64_t password_updated_at_us = 0;
    uint64_t last_login_at_us = 0;
};

struct ComputeSlotPolicy {
    std::string slot_id;
    std::string access_mode;
    std::string dedicated_username;
    uint64_t updated_at_us = 0;
};

class AuthManager {
public:
    static AuthManager& instance();

    bool bootstrapRequired() const;
    std::string bootstrapMode() const;
    std::string sharedDbPath() const;
    std::string cookieName() const;

    std::optional<AuthenticatedUser> authenticateRequest(
        const drogon::HttpRequestPtr& req);
    std::optional<AuthenticatedUser> authenticateSessionToken(
        const std::string& session_token);

    AuthResult bootstrapAdmin(
        const std::string& username,
        const std::string& display_name,
        const std::string& password,
        const std::string& bootstrap_password);

    AuthResult login(const std::string& username, const std::string& password);
    void logout(const drogon::HttpRequestPtr& req, drogon::HttpResponsePtr& resp);

    std::vector<AuthUserSummary> listUsers() const;

    AuthResult createUser(
        const std::string& actor_username,
        const std::string& username,
        const std::string& display_name,
        const std::string& password,
        bool is_admin,
        bool enabled,
        bool shared_compute_access);

    AuthResult updateUser(
        const std::string& actor_username,
        const std::string& username,
        const std::optional<std::string>& display_name,
        const std::optional<std::string>& password,
        const std::optional<bool>& is_admin,
        const std::optional<bool>& enabled,
        const std::optional<bool>& shared_compute_access);

    AuthResult deleteUser(
        const std::string& actor_username,
        const std::string& username);

    std::vector<ComputeSlotPolicy> listComputeSlotPolicies() const;
    AuthResult setComputeSlotPolicy(
        const std::string& actor_username,
        const std::string& slot_id,
        const std::string& access_mode,
        const std::optional<std::string>& dedicated_username);

    void applySessionCookie(
        const std::optional<std::string>& session_token,
        drogon::HttpResponsePtr& resp) const;
    void clearSessionCookie(drogon::HttpResponsePtr& resp) const;

private:
    struct StoredUser {
        std::string username;
        std::string display_name;
        std::string password_salt;
        std::string password_hash;
        bool is_admin = false;
        bool enabled = true;
        bool shared_compute_access = false;
        uint64_t created_at_us = 0;
        uint64_t updated_at_us = 0;
        uint64_t password_updated_at_us = 0;
        uint64_t last_login_at_us = 0;
    };

    struct StoredSession {
        std::string token;
        std::string username;
        uint64_t created_at_us = 0;
        uint64_t expires_at_us = 0;
    };

    AuthManager();

    AuthManager(const AuthManager&) = delete;
    AuthManager& operator=(const AuthManager&) = delete;

    std::optional<std::string> extractSessionToken(
        const drogon::HttpRequestPtr& req) const;

    AuthenticatedUser toAuthenticatedUser(const StoredUser& user) const;
    AuthUserSummary toUserSummary(const StoredUser& user) const;

    void initializeDatabaseLocked() const;
    bool anyUsersLocked() const;
    void pruneExpiredSessionsLocked() const;
    void ensureBootstrapStateLocked();

    std::optional<StoredUser> loadUserLocked(const std::string& username) const;
    std::optional<StoredSession> loadSessionLocked(const std::string& token) const;
    std::vector<StoredUser> loadAllUsersLocked() const;
    std::vector<ComputeSlotPolicy> loadAllComputeSlotPoliciesLocked() const;

    void saveUserLocked(const StoredUser& user) const;
    void saveSessionLocked(const StoredSession& session) const;
    void saveComputeSlotPolicyLocked(const ComputeSlotPolicy& policy) const;
    void deleteUserLocked(const std::string& username) const;
    void deleteSessionsForUserLocked(const std::string& username) const;
    void deleteSessionLocked(const std::string& token) const;
    bool enabledAdminExistsOtherThanLocked(const std::string& username) const;

    bool validateUsername(const std::string& username) const;
    bool validatePassword(const std::string& password) const;
    std::string createRandomToken(size_t byte_count) const;
    std::string hashPassword(
        const std::string& salt,
        const std::string& password) const;
    bool passwordsMatch(
        const std::string& lhs,
        const std::string& rhs) const;
    AuthResult buildSessionForUserLocked(StoredUser user);

    mutable std::mutex mutex_;
    std::string db_path_;
    std::string cookie_name_;
    std::string bootstrap_mode_;
    std::string bootstrap_password_;
    uint64_t session_ttl_us_ = 24ULL * 60ULL * 60ULL * 1000000ULL;
};
