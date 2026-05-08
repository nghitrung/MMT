#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course,
# and is released under the "MIT License Agreement".
#
# AsynapRous release
#
# The authors hereby grant to Licensee personal permission
# to use and modify the Licensed Source Code for the sole
# purpose of studying while attending the course
#


"""
app.sampleapp
~~~~~~~~~~~~~

P2P chat application built on the AsynapRous framework.

All socket operations use ``setblocking(False)`` with
explicit ``BlockingIOError`` handling.  Chat messages are
stored as immutable ``ChatMessage`` namedtuples in an
append-only ``MessageStore`` (no modify, no delete).

Routes:
    POST  /login    — Authenticate and register with Tracker
    POST  /logout   — Unregister from Tracker
    GET   /get-list — List online peers
    POST  /echo     — Echo JSON payload
    PUT   /hello    — Async greeting (demo)
    POST  /message  — Receive P2P chat message

Client utilities:
    send_p2p_message  — Direct non-blocking P2P message
    broadcast_message — Threaded broadcast to all peers
"""

import sys
import os
import json
import socket
import select
import threading
import datetime
import base64
from collections import namedtuple

from daemon import AsynapRous
from daemon.utils import nb_connect, nb_sendall, nb_recv_all


# ---------------------------------------------------------------
# Immutable message type (namedtuple = immutable by nature)
# ---------------------------------------------------------------

ChatMessage = namedtuple("ChatMessage", [
    "msg_id",
    "sender",
    "recipient",
    "content",
    "timestamp",
])
"""Immutable chat message record.

Fields cannot be modified or deleted after creation,
satisfying the immutable-data constraint.
"""


class MessageStore:
    """Append-only, thread-safe message store.

    Messages are :class:`ChatMessage` namedtuples and cannot
    be modified or deleted once appended.
    """

    def __init__(self):
        self._messages = []
        self._lock = threading.Lock()
        self._counter = 0

    def append(self, sender, recipient, content):
        """Create and store an immutable ChatMessage.

        :param sender: Username of the sender.
        :param recipient: Username of the recipient.
        :param content: Message text.
        :rtype: ChatMessage
        """
        with self._lock:
            self._counter += 1
            msg = ChatMessage(
                msg_id=self._counter,
                sender=sender,
                recipient=recipient,
                content=content,
                timestamp=(
                    datetime.datetime.utcnow().isoformat()
                ),
            )
            self._messages.append(msg)
        return msg

    def get_all(self):
        """Return an immutable copy of all messages.

        :rtype: tuple[ChatMessage]
        """
        with self._lock:
            return tuple(self._messages)

    def get_by_user(self, username):
        """Return messages involving a user.

        :rtype: tuple[ChatMessage]
        """
        with self._lock:
            return tuple(
                m for m in self._messages
                if m.sender == username
                or m.recipient == username
                or m.recipient == "self"
            )

    def count(self):
        """Return total number of stored messages."""
        with self._lock:
            return len(self._messages)


# ---------------------------------------------------------------
# Application instance and shared state
# ---------------------------------------------------------------

app = AsynapRous()

_sessions = {}
_sessions_lock = threading.Lock()

message_store = MessageStore()

TRACKER_HOST = os.environ.get("TRACKER_HOST", "127.0.0.1")
TRACKER_PORT = int(os.environ.get("TRACKER_PORT", "8080"))

# Demo credential store
_USER_DB = {
    "alice": "password123",
    "bob": "secret456",
    "charlie": "pass789",
    "guest": "guest",
}


# ---------------------------------------------------------------
# Helper: non-blocking HTTP request to Tracker
# ---------------------------------------------------------------

def _http_request(host, port, method, path,
                  body_dict=None, timeout=5):
    """Send a minimal HTTP request using non-blocking I/O.

    :param host: Target host IP.
    :param port: Target port.
    :param method: HTTP method.
    :param path: URL path.
    :param body_dict: JSON body dict (optional).
    :param timeout: Socket timeout in seconds.
    :rtype: str — response body text.
    """
    sock = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )

    try:
        # Non-blocking connect
        nb_connect(sock, (host, port), timeout=timeout)

        body_bytes = b""
        if body_dict is not None:
            body_bytes = json.dumps(
                body_dict
            ).encode("utf-8")

        request_line = "{} {} HTTP/1.1\r\n".format(
            method.upper(), path
        )
        headers = (
            "Host: tracker\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: {}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).format(len(body_bytes))

        payload = (
            (request_line + headers).encode("utf-8")
            + body_bytes
        )

        # Non-blocking send
        nb_sendall(sock, payload, timeout=timeout)

        # Non-blocking receive
        response = nb_recv_all(
            sock, timeout=timeout, bufsize=4096
        )

        decoded = response.decode(
            "utf-8", errors="replace"
        )

        parts = decoded.split("\r\n\r\n", 1)
        return parts[1] if len(parts) > 1 else ""

    except BlockingIOError:
        print("[SampleApp] BlockingIOError in _http_request")
        return json.dumps({"error": "BlockingIOError"})
    except (socket.timeout, socket.error) as exc:
        print(
            "[SampleApp] _http_request error: "
            "{}".format(exc)
        )
        return json.dumps({"error": str(exc)})
    finally:
        sock.close()


def _register_peer_on_tracker(peer_ip, peer_port,
                              username):
    """Call POST /peers/register on Tracker.

    :rtype: dict
    """
    body = {
        "ip": peer_ip,
        "port": peer_port,
        "username": username,
    }
    raw = _http_request(
        TRACKER_HOST, TRACKER_PORT,
        "POST", "/peers/register", body,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"error": "Invalid response", "raw": raw}


def _unregister_peer_on_tracker(peer_ip, peer_port):
    """Call POST /peers/unregister on Tracker.

    :rtype: dict
    """
    body = {"ip": peer_ip, "port": peer_port}
    raw = _http_request(
        TRACKER_HOST, TRACKER_PORT,
        "POST", "/peers/unregister", body,
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {"error": "Invalid response", "raw": raw}


def _fetch_peer_list():
    """Call GET /peers on Tracker.

    :rtype: dict
    """
    raw = _http_request(
        TRACKER_HOST, TRACKER_PORT, "GET", "/peers"
    )
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {
            "peers": [], "count": 0,
            "error": "Invalid response",
        }


# ---------------------------------------------------------------
# Route: /login (POST)
# ---------------------------------------------------------------

@app.route('/login', methods=['POST'])
def login(headers="guest", body="anonymous"):
    """Authenticate user and register peer on Tracker.

    Supports three auth methods (in priority order):
    1. JSON body credentials
    2. Cookie: session=<username>
    3. Authorization: Basic <base64>

    :param headers: Parsed HTTP headers (dict).
    :param body: Raw HTTP body (JSON string).
    :rtype: bytes
    """
    print("[SampleApp] /login invoked")

    username = None
    password = None
    peer_ip = "127.0.0.1"
    peer_port = 9000

    # 1. JSON body
    if body and body.strip():
        try:
            data = json.loads(body)
            username = data.get("username")
            password = data.get("password")
            peer_ip = data.get("peer_ip", peer_ip)
            peer_port = int(
                data.get("peer_port", peer_port)
            )
        except (json.JSONDecodeError, ValueError):
            pass

    # 2. Cookie fallback
    if not username and isinstance(headers, dict):
        cookie_str = headers.get("cookie", "")
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if pair.startswith("session="):
                username = pair.split("=", 1)[1]
                password = _USER_DB.get(username)
                break

    # 3. Basic Auth fallback
    if not username and isinstance(headers, dict):
        auth = headers.get("authorization", "")
        if auth.lower().startswith("basic "):
            try:
                decoded = base64.b64decode(
                    auth[6:]
                ).decode("utf-8")
                username, password = decoded.split(":", 1)
            except Exception:
                pass

    # Validate
    if not username or not password:
        return json.dumps({
            "status": "error",
            "message": "Missing username or password",
        }).encode("utf-8")

    expected = _USER_DB.get(username)
    if expected is None or expected != password:
        return json.dumps({
            "status": "error",
            "message": "Invalid credentials",
        }).encode("utf-8")

    # Register session
    session_id = username
    with _sessions_lock:
        _sessions[username] = {
            "username": username,
            "peer_ip": peer_ip,
            "peer_port": peer_port,
            "logged_in_at": (
                datetime.datetime.utcnow().isoformat()
            ),
        }

    # Register on Tracker
    tracker_resp = _register_peer_on_tracker(
        peer_ip, peer_port, username
    )
    print(
        "[SampleApp] Tracker reg: {}".format(tracker_resp)
    )

    resp = {
        "status": "ok",
        "message": "Welcome, {}!".format(username),
        "session": session_id,
        "set-cookie": "session={}".format(session_id),
        "tracker": tracker_resp,
    }
    return json.dumps(resp).encode("utf-8")


# ---------------------------------------------------------------
# Route: /logout (POST)
# ---------------------------------------------------------------

@app.route('/logout', methods=['POST'])
def logout(headers="guest", body="anonymous"):
    """Unregister peer from Tracker and clear session.

    :param headers: Parsed HTTP headers.
    :param body: JSON body.
    :rtype: bytes
    """
    print("[SampleApp] /logout invoked")

    username = None
    peer_ip = "127.0.0.1"
    peer_port = 9000

    if body and body.strip():
        try:
            data = json.loads(body)
            username = data.get("username")
            peer_ip = data.get("peer_ip", peer_ip)
            peer_port = int(
                data.get("peer_port", peer_port)
            )
        except (json.JSONDecodeError, ValueError):
            pass

    if not username and isinstance(headers, dict):
        cookie_str = headers.get("cookie", "")
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if pair.startswith("session="):
                username = pair.split("=", 1)[1]
                break

    if not username:
        return json.dumps({
            "status": "error",
            "message": "Missing username",
        }).encode("utf-8")

    with _sessions_lock:
        info = _sessions.pop(username, None)
        if info:
            peer_ip = info.get("peer_ip", peer_ip)
            peer_port = info.get("peer_port", peer_port)

    tracker_resp = _unregister_peer_on_tracker(
        peer_ip, peer_port
    )

    return json.dumps({
        "status": "ok",
        "message": "Goodbye, {}!".format(username),
        "tracker": tracker_resp,
    }).encode("utf-8")


# ---------------------------------------------------------------
# Route: /get-list (GET)
# ---------------------------------------------------------------

@app.route('/get-list', methods=['GET'])
def get_list(headers="guest", body="anonymous"):
    """Return JSON list of online peers from Tracker.

    :param headers: Parsed HTTP headers.
    :param body: Ignored for GET.
    :rtype: bytes
    """
    print("[SampleApp] /get-list invoked")

    peer_data = _fetch_peer_list()
    print(
        "[SampleApp] Tracker: {} peer(s)".format(
            peer_data.get("count", 0)
        )
    )

    return json.dumps({
        "status": "ok",
        "peers": peer_data.get("peers", []),
        "count": peer_data.get("count", 0),
    }, indent=2).encode("utf-8")


# ---------------------------------------------------------------
# Route: /echo (POST)
# ---------------------------------------------------------------

@app.route("/echo", methods=["POST"])
def echo(headers="guest", body="anonymous"):
    """Echo back received JSON payload.

    :rtype: bytes
    """
    print("[SampleApp] /echo body {}".format(body))
    try:
        message = json.loads(body)
        return json.dumps(
            {"received": message}
        ).encode("utf-8")
    except json.JSONDecodeError:
        return json.dumps(
            {"error": "Invalid JSON"}
        ).encode("utf-8")


# ---------------------------------------------------------------
# Route: /hello (PUT, async)
# ---------------------------------------------------------------

@app.route('/hello', methods=['PUT'])
async def hello(headers, body):
    """Async greeting endpoint.

    :rtype: bytes
    """
    print(
        "[SampleApp] ['PUT'] **ASYNC** "
        "Hello {} to {}".format(headers, body)
    )
    data = {
        "id": 1,
        "name": "Alice",
        "email": "alice@example.com",
    }
    return json.dumps(data).encode("utf-8")


# ---------------------------------------------------------------
# Route: /message (POST) — receive P2P message
# ---------------------------------------------------------------

@app.route('/message', methods=['POST'])
def receive_message(headers="guest", body="anonymous"):
    """Receive an incoming P2P chat message.

    The message is stored in the immutable MessageStore
    and cannot be modified or deleted afterwards.

    :param headers: Parsed HTTP headers.
    :param body: JSON body with ``from`` and ``message``.
    :rtype: bytes
    """
    print("[SampleApp] /message received")

    try:
        data = json.loads(body)
        sender = data.get("from", "unknown")
        content = data.get("message", "")
        timestamp = datetime.datetime.utcnow().strftime(
            "%H:%M:%S"
        )

        # Store as immutable ChatMessage (append-only)
        msg = message_store.append(
            sender=sender,
            recipient="self",
            content=content,
        )

        print("=" * 50)
        print(
            "[{}] Message from {}: {}".format(
                timestamp, sender, content
            )
        )
        print(
            "[Store] msg_id={} total={}".format(
                msg.msg_id, message_store.count()
            )
        )
        print("=" * 50)

        resp = {
            "status": "delivered",
            "from": sender,
            "msg_id": msg.msg_id,
            "received_at": timestamp,
        }
    except json.JSONDecodeError:
        resp = {
            "status": "error",
            "message": "Invalid JSON body",
        }

    return json.dumps(resp).encode("utf-8")


# ---------------------------------------------------------------
# Route: /history (GET) — fetch message history
# ---------------------------------------------------------------

@app.route('/history', methods=['GET'])
def get_history(headers="guest", body="anonymous"):
    """Return all chat messages involving the current session user.

    :rtype: bytes
    """
    print("[SampleApp] /history invoked")

    # Extract username from cookie
    username = None
    if isinstance(headers, dict):
        cookie_str = headers.get("cookie", "")
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if pair.startswith("session="):
                username = pair.split("=", 1)[1]
                break

    if not username:
        return json.dumps({
            "status": "error",
            "message": "Not authenticated",
        }).encode("utf-8")

    # Get messages
    msgs = message_store.get_by_user(username)
    
    # Format messages
    history = []
    for m in msgs:
        history.append({
            "msg_id": m.msg_id,
            "sender": m.sender,
            "recipient": m.recipient,
            "content": m.content,
            "timestamp": m.timestamp,
        })

    return json.dumps({
        "status": "ok",
        "messages": history
    }).encode("utf-8")


# ---------------------------------------------------------------
# Route: /send (POST) — Frontend initiates P2P message
# ---------------------------------------------------------------

@app.route('/send', methods=['POST'])
def handle_send(headers="guest", body="anonymous"):
    """Handle frontend request to send a P2P message.
    
    Body should contain {"to": "username", "message": "content"}
    :rtype: bytes
    """
    print("[SampleApp] /send invoked")
    
    # Authenticate
    username = None
    if isinstance(headers, dict):
        cookie_str = headers.get("cookie", "")
        for pair in cookie_str.split(";"):
            pair = pair.strip()
            if pair.startswith("session="):
                username = pair.split("=", 1)[1]
                break

    if not username:
        return json.dumps({"status": "error", "message": "Not authenticated"}).encode("utf-8")
        
    try:
        data = json.loads(body)
        target_user = data.get("to")
        text = data.get("message")
    except Exception:
        return json.dumps({"status": "error", "message": "Invalid JSON"}).encode("utf-8")
        
    if not target_user or not text:
        return json.dumps({"status": "error", "message": "Missing 'to' or 'message'"}).encode("utf-8")
        
    # Look up peer in tracker
    peer_data = _fetch_peer_list()
    target = None
    for p in peer_data.get("peers", []):
        if p.get("username") == target_user:
            target = p
            break
            
    if not target:
        return json.dumps({"status": "error", "message": "Peer not online"}).encode("utf-8")
        
    # Send P2P message (this adds to MessageStore automatically)
    result = send_p2p_message(
        target["ip"], int(target["port"]), username, text, target_user=target_user
    )
    
    return json.dumps(result).encode("utf-8")


# ---------------------------------------------------------------
# P2P: send direct message (non-blocking socket)
# ---------------------------------------------------------------



def send_p2p_message(peer_ip, peer_port,
                     sender_name, message, target_user="peer", timeout=5):
    """Send a direct P2P message via non-blocking socket.

    The socket uses ``setblocking(False)`` throughout.
    ``BlockingIOError`` is caught explicitly during both
    send and receive phases.

    :param peer_ip: Target peer IP.
    :param peer_port: Target peer port.
    :param sender_name: Sender's username.
    :param message: Chat message text.
    :param timeout: Socket timeout in seconds.
    :rtype: dict — parsed response or error dict.
    """
    sock = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )

    try:
        # Non-blocking connect
        nb_connect(
            sock, (peer_ip, int(peer_port)),
            timeout=timeout,
        )

        # Build immutable message payload
        msg = ChatMessage(
            msg_id=0,
            sender=sender_name,
            recipient=target_user,
            content=message,
            timestamp=(
                datetime.datetime.utcnow().isoformat()
            ),
        )

        # Store locally (append-only, immutable)
        stored = message_store.append(
            sender=msg.sender,
            recipient=msg.recipient,
            content=msg.content,
        )

        payload = json.dumps({
            "from": msg.sender,
            "message": msg.content,
            "timestamp": msg.timestamp,
            "msg_id": stored.msg_id,
        })
        payload_bytes = payload.encode("utf-8")

        http_request = (
            "POST /message HTTP/1.1\r\n"
            "Host: {}:{}\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: {}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).format(peer_ip, peer_port, len(payload_bytes))

        # Non-blocking send
        nb_sendall(
            sock,
            http_request.encode("utf-8") + payload_bytes,
            timeout=timeout,
        )

        # Non-blocking receive
        response_data = nb_recv_all(
            sock, timeout=timeout, bufsize=4096
        )

        decoded = response_data.decode(
            "utf-8", errors="replace"
        )
        parts = decoded.split("\r\n\r\n", 1)
        body_text = parts[1] if len(parts) > 1 else ""

        try:
            return json.loads(body_text)
        except json.JSONDecodeError:
            return {
                "status": "unknown",
                "raw_response": body_text,
            }

    except BlockingIOError:
        print(
            "[P2P] BlockingIOError to "
            "{}:{}".format(peer_ip, peer_port)
        )
        return {
            "status": "error",
            "message": "BlockingIOError",
        }
    except socket.timeout:
        print(
            "[P2P] Timeout to "
            "{}:{}".format(peer_ip, peer_port)
        )
        return {
            "status": "error",
            "message": "Connection timed out",
        }
    except socket.error as exc:
        print(
            "[P2P] Socket error to {}:{} "
            "— {}".format(peer_ip, peer_port, exc)
        )
        return {"status": "error", "message": str(exc)}
    finally:
        sock.close()


# ---------------------------------------------------------------
# Broadcast: send to all peers concurrently
# ---------------------------------------------------------------

def broadcast_message(peer_list, sender_name, message,
                      exclude_self=None):
    """Broadcast a message to all peers via threads.

    Each peer gets its own daemon thread for concurrent
    delivery.  Results are collected thread-safely.

    :param peer_list: List of peer dicts.
    :param sender_name: Sender's username.
    :param message: Chat message text.
    :param exclude_self: (ip, port) to skip.
    :rtype: list[dict]
    """
    results = []
    results_lock = threading.Lock()
    threads = []

    def _send_to_peer(peer):
        peer_ip = peer.get("ip")
        peer_port = int(peer.get("port", 9000))
        peer_user = peer.get("username", "unknown")

        if exclude_self:
            if (peer_ip, peer_port) == exclude_self:
                return

        print(
            "[Broadcast] -> {} ({}:{})".format(
                peer_user, peer_ip, peer_port
            )
        )

        result = send_p2p_message(
            peer_ip, peer_port, sender_name, message
        )
        result["target_peer"] = peer_user
        result["target_addr"] = "{}:{}".format(
            peer_ip, peer_port
        )

        with results_lock:
            results.append(result)

    for peer in peer_list:
        thr = threading.Thread(
            target=_send_to_peer,
            args=(peer,),
            daemon=True,
        )
        threads.append(thr)
        thr.start()

    for thr in threads:
        thr.join(timeout=10)

    print(
        "[Broadcast] Sent to {}/{} peer(s)".format(
            len(results), len(peer_list)
        )
    )
    return results


# ---------------------------------------------------------------
# Interactive Chat Client
# ---------------------------------------------------------------

def run_chat_client(username, password, my_ip, my_port,
                    tracker_host=None, tracker_port=None):
    """Run an interactive P2P chat client in terminal.

    Commands:
        list              — show online peers
        msg <user> <text> — direct message
        broadcast <text>  — send to all peers
        history           — show local message history
        quit              — logout and exit

    :param username: Username for login.
    :param password: Password for login.
    :param my_ip: This client's IP.
    :param my_port: This client's listening port.
    :param tracker_host: Override Tracker host.
    :param tracker_port: Override Tracker port.
    """
    global TRACKER_HOST, TRACKER_PORT
    if tracker_host:
        TRACKER_HOST = tracker_host
    if tracker_port:
        TRACKER_PORT = tracker_port

    print("=" * 60)
    print("  AsynapRous P2P Chat Client")
    print("  User: {}  |  {}:{}".format(
        username, my_ip, my_port
    ))
    print("=" * 60)

    # Login
    login_body = {
        "username": username,
        "password": password,
        "peer_ip": my_ip,
        "peer_port": my_port,
    }
    login_resp = _http_request(
        TRACKER_HOST, TRACKER_PORT,
        "POST", "/peers/register", login_body,
    )
    print("[Client] Login: {}".format(login_resp))

    try:
        while True:
            cmd = input(
                "\n[{}] > ".format(username)
            ).strip()
            if not cmd:
                continue

            parts = cmd.split(None, 2)
            action = parts[0].lower()

            if action in ("quit", "exit"):
                _unregister_peer_on_tracker(
                    my_ip, my_port
                )
                print("[Client] Goodbye!")
                break

            elif action == "list":
                peer_data = _fetch_peer_list()
                peers = peer_data.get("peers", [])
                if not peers:
                    print("  (no peers online)")
                else:
                    print("  Online ({})".format(
                        len(peers)
                    ))
                    for p in peers:
                        print(
                            "    - {} @ {}:{}".format(
                                p.get("username", "?"),
                                p.get("ip", "?"),
                                p.get("port", "?"),
                            )
                        )

            elif action == "msg" and len(parts) >= 3:
                target_user = parts[1]
                text = parts[2]
                peer_data = _fetch_peer_list()
                target = None
                for p in peer_data.get("peers", []):
                    if p.get("username") == target_user:
                        target = p
                        break
                if not target:
                    print(
                        "  '{}' not found".format(
                            target_user
                        )
                    )
                else:
                    result = send_p2p_message(
                        target["ip"],
                        int(target["port"]),
                        username, text,
                    )
                    print("  -> {}".format(
                        result.get("status", "?")
                    ))

            elif action == "broadcast" and len(parts) >= 2:
                text = cmd.split(None, 1)[1]
                peer_data = _fetch_peer_list()
                peers = peer_data.get("peers", [])
                results = broadcast_message(
                    peers, username, text,
                    exclude_self=(my_ip, my_port),
                )
                for r in results:
                    print("  -> {}: {}".format(
                        r.get("target_peer", "?"),
                        r.get("status", "?"),
                    ))

            elif action == "history":
                msgs = message_store.get_all()
                if not msgs:
                    print("  (no messages)")
                else:
                    for m in msgs:
                        print(
                            "  [{}] {} -> {}: "
                            "{}".format(
                                m.timestamp,
                                m.sender,
                                m.recipient,
                                m.content,
                            )
                        )

            else:
                print(
                    "  Commands: list | msg <user> "
                    "<text> | broadcast <text> | "
                    "history | quit"
                )

    except KeyboardInterrupt:
        _unregister_peer_on_tracker(my_ip, my_port)
        print("\n[Client] Interrupted. Goodbye!")


# ---------------------------------------------------------------
# Server entry point
# ---------------------------------------------------------------

def create_sampleapp(ip, port):
    """Launch the RESTful chat application.

    :param ip: IP address to bind.
    :param port: Port number to listen on.
    """
    app.prepare_address(ip, port)
    app.run()


# ---------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        prog="SampleApp",
        description="AsynapRous P2P Chat",
    )
    parser.add_argument(
        "--mode",
        choices=["server", "client"],
        default="server",
    )
    parser.add_argument("--ip", default="0.0.0.0")
    parser.add_argument(
        "--port", type=int, default=2026
    )
    parser.add_argument(
        "--username", default="guest"
    )
    parser.add_argument(
        "--password", default="guest"
    )
    parser.add_argument(
        "--tracker-host", default="127.0.0.1"
    )
    parser.add_argument(
        "--tracker-port", type=int, default=8080
    )

    args = parser.parse_args()

    if args.mode == "server":
        create_sampleapp(args.ip, args.port)
    else:
        run_chat_client(
            username=args.username,
            password=args.password,
            my_ip=args.ip,
            my_port=args.port,
            tracker_host=args.tracker_host,
            tracker_port=args.tracker_port,
        )
