from __future__ import annotations

import os
import sqlite3
from dataclasses import dataclass
from http.cookies import SimpleCookie
from http.cookiejar import CookieJar
from pathlib import Path
from typing import Any
from urllib import parse, request
import inspect
import json


@dataclass(slots=True)
class AuthenticatedSessionUser:
    username: str
    display_name: str
    is_admin: bool
    shared_compute_access: bool = False


@dataclass(slots=True)
class ComputeSlotPolicy:
    slot_id: str
    access_mode: str
    dedicated_username: str | None
    updated_at_us: int


class SharedAuthStore:
    def __init__(self, db_path: str | None = None, cookie_name: str | None = None) -> None:
        self.db_path = db_path or os.getenv("NATKIT_AUTH_DB_PATH", "uploads/auth.sqlite3")
        self.cookie_name = cookie_name or os.getenv("NATKIT_AUTH_COOKIE_NAME", "natkit_session")

    def authenticate_session_token(
        self, session_token: str
    ) -> AuthenticatedSessionUser | None:
        if not session_token:
            return None
        db_file = Path(self.db_path)
        if not db_file.exists():
            return None
        with sqlite3.connect(str(db_file)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                "DELETE FROM auth_sessions WHERE expires_at_us <= "
                "CAST((julianday('now') - 2440587.5) * 86400000000 AS INTEGER)"
            )
            row = connection.execute(
                """
                SELECT u.username, u.display_name, u.is_admin, u.shared_compute_access
                FROM auth_sessions s
                JOIN auth_users u ON u.username = s.username
                WHERE s.token = ? AND u.enabled = 1
                """,
                (session_token,),
            ).fetchone()
            if row is None:
                return None
            return AuthenticatedSessionUser(
                username=str(row["username"]),
                display_name=str(row["display_name"]),
                is_admin=bool(row["is_admin"]),
                shared_compute_access=bool(row["shared_compute_access"]),
            )

    def authenticate_cookie_header(
        self, cookie_header: str | None
    ) -> AuthenticatedSessionUser | None:
        if not cookie_header:
            return None
        cookie = SimpleCookie()
        cookie.load(cookie_header)
        morsel = cookie.get(self.cookie_name)
        if morsel is None:
            return None
        return self.authenticate_session_token(morsel.value)

    def list_compute_slot_policies(self) -> list[ComputeSlotPolicy]:
        db_file = Path(self.db_path)
        if not db_file.exists():
            return []
        with sqlite3.connect(str(db_file)) as connection:
            connection.row_factory = sqlite3.Row
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS compute_slot_policies (
                    slot_id TEXT PRIMARY KEY,
                    access_mode TEXT NOT NULL,
                    dedicated_username TEXT,
                    updated_at_us INTEGER NOT NULL
                )
                """
            )
            rows = connection.execute(
                """
                SELECT slot_id, access_mode, dedicated_username, updated_at_us
                FROM compute_slot_policies
                ORDER BY slot_id ASC
                """
            ).fetchall()
        return [
            ComputeSlotPolicy(
                slot_id=str(row["slot_id"]),
                access_mode=str(row["access_mode"]),
                dedicated_username=(
                    str(row["dedicated_username"])
                    if row["dedicated_username"] is not None
                    else None
                ),
                updated_at_us=int(row["updated_at_us"] or 0),
            )
            for row in rows
        ]

    def set_compute_slot_policy(
        self,
        *,
        slot_id: str,
        access_mode: str,
        dedicated_username: str | None,
        updated_at_us: int,
    ) -> None:
        db_file = Path(self.db_path)
        db_file.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_file)) as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS compute_slot_policies (
                    slot_id TEXT PRIMARY KEY,
                    access_mode TEXT NOT NULL,
                    dedicated_username TEXT,
                    updated_at_us INTEGER NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO compute_slot_policies
                    (slot_id, access_mode, dedicated_username, updated_at_us)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(slot_id) DO UPDATE SET
                    access_mode = excluded.access_mode,
                    dedicated_username = excluded.dedicated_username,
                    updated_at_us = excluded.updated_at_us
                """,
                (slot_id, access_mode, dedicated_username, updated_at_us),
            )
            connection.commit()


def websocket_cookie_header(websocket: Any) -> str | None:
    request = getattr(websocket, "request", None)
    headers = getattr(request, "headers", None)
    if headers is None:
        headers = getattr(websocket, "request_headers", None)
    if headers is None:
        return None
    getter = getattr(headers, "get", None)
    if callable(getter):
        return getter("Cookie") or getter("cookie")
    return None


def derive_auth_base_url(control_plane_url: str) -> str:
    parsed = parse.urlparse(control_plane_url)
    scheme = "https" if parsed.scheme == "wss" else "http"
    host = parsed.hostname or "127.0.0.1"
    return f"{scheme}://{host}:7409"


def login_for_session_token(
    *,
    auth_base_url: str,
    username: str,
    password: str,
    cookie_name: str = "natkit_session",
) -> str:
    cookie_jar = CookieJar()
    opener = request.build_opener(request.HTTPCookieProcessor(cookie_jar))
    payload = json.dumps({"username": username, "password": password}).encode("utf-8")
    req = request.Request(
        parse.urljoin(auth_base_url.rstrip("/") + "/", "api/auth/login"),
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with opener.open(req) as response:
        response.read()
    for cookie in cookie_jar:
        if cookie.name == cookie_name:
            return str(cookie.value)
    raise RuntimeError(f"login succeeded but {cookie_name} cookie was not returned")


def resolve_session_token(
    *,
    session_token: str | None,
    username: str | None,
    password: str | None,
    auth_base_url: str | None,
    cookie_name: str = "natkit_session",
) -> str | None:
    if session_token:
        return session_token
    if username and password:
        return login_for_session_token(
            auth_base_url=auth_base_url or derive_auth_base_url("ws://127.0.0.1:8786"),
            username=username,
            password=password,
            cookie_name=cookie_name,
        )
    return None


def build_websocket_cookie_header(
    *,
    session_token: str | None,
    cookie_name: str = "natkit_session",
) -> dict[str, str]:
    if not session_token:
        return {}
    return {"Cookie": f"{cookie_name}={session_token}"}


def apply_websocket_headers(
    connect_factory: Any,
    headers: dict[str, str],
) -> dict[str, Any]:
    if not headers:
        return {}
    try:
        signature = inspect.signature(connect_factory)
    except (TypeError, ValueError):
        return {"additional_headers": headers}
    if "additional_headers" in signature.parameters:
        return {"additional_headers": headers}
    if "extra_headers" in signature.parameters:
        return {"extra_headers": headers}
    return {"additional_headers": headers}
