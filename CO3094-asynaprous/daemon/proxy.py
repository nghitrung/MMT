#
# Copyright (C) 2026 pdnguyen of HCMC University of Technology VNU-HCM.
# All rights reserved.
# This file is part of the CO3093/CO3094 course.
#
# AsynapRous release
#
# The authors hereby grant to Licensee personal permission to use
# and modify the Licensed Source Code for the sole purpose of studying
# while attending the course
#

"""
daemon.proxy
~~~~~~~~~~~~~

Proxy server that routes HTTP requests to backend services
based on hostname mappings loaded from ``config/proxy.conf``.

All socket operations use ``setblocking(False)`` with
explicit ``BlockingIOError`` handling via
:mod:`daemon.utils`.

Features:
    - Config-driven hostname → backend mapping.
    - PeerTracker in-memory registry for active peers.
    - Round-robin load balancing for multi-backend hosts.
    - Multi-threaded, non-blocking client handling.
"""

import socket
import select
import threading
import json
import re
import os
import datetime

from .response import *
from .httpadapter import HttpAdapter
from .dictionary import CaseInsensitiveDict
from .utils import nb_connect, nb_sendall, nb_recv_all, nb_accept


# ---------------------------------------------------------------
# Configuration Loader
# ---------------------------------------------------------------

def load_proxy_config(config_path="config/proxy.conf"):
    """Parse proxy config and build route mapping.

    :param config_path: Path to the configuration file.
    :rtype: dict — hostname → (targets, policy).
    """
    routes = {}

    if not os.path.isfile(config_path):
        print(
            "[Proxy] Config '{}' not found"
            " — empty routes".format(config_path)
        )
        return routes

    with open(config_path, "r") as fh:
        config_text = fh.read()

    host_blocks = re.findall(
        r'host\s+"([^"]+)"\s*\{(.*?)\}',
        config_text, re.DOTALL,
    )

    for host, block in host_blocks:
        proxy_passes = re.findall(
            r'proxy_pass\s+http://([^\s;]+);', block
        )
        policy_match = re.search(
            r'dist_policy\s+(\S+)', block
        )
        dist_policy = (
            policy_match.group(1) if policy_match
            else "round-robin"
        )

        if len(proxy_passes) == 1:
            routes[host] = (proxy_passes[0], dist_policy)
        else:
            routes[host] = (proxy_passes, dist_policy)

    print(
        "[Proxy] Loaded {} route(s) from {}".format(
            len(routes), config_path
        )
    )
    for host, (targets, policy) in routes.items():
        print(
            "   + '{}' -> {} policy={}".format(
                host, targets, policy
            )
        )

    return routes


# ---------------------------------------------------------------
# Peer Tracker — thread-safe in-memory registry
# ---------------------------------------------------------------

class PeerTracker:
    """Thread-safe registry of active peers.

    Each peer is keyed by ``(ip, port)`` and stores
    username + timestamp metadata.
    """

    def __init__(self):
        """Initialise empty peer registry."""
        self._peers = {}
        self._lock = threading.Lock()

    def register(self, ip, port, username="anonymous"):
        """Register or update a peer.

        :param ip: Peer IP address.
        :param port: Peer port number.
        :param username: Display name.
        """
        with self._lock:
            self._peers[(ip, port)] = {
                "ip": ip,
                "port": port,
                "username": username,
                "registered_at": (
                    datetime.datetime.utcnow().isoformat()
                ),
            }
        print(
            "[Tracker] Registered {}:{} (user={})".format(
                ip, port, username
            )
        )

    def unregister(self, ip, port):
        """Remove a peer.

        :rtype: bool
        """
        with self._lock:
            removed = self._peers.pop((ip, port), None)
        if removed:
            print(
                "[Tracker] Unregistered {}:{}".format(
                    ip, port
                )
            )
        return removed is not None

    def get_peers(self):
        """Return snapshot list of all active peers.

        :rtype: list[dict]
        """
        with self._lock:
            return list(self._peers.values())

    def get_peer(self, ip, port):
        """Look up a single peer.

        :rtype: dict or None
        """
        with self._lock:
            return self._peers.get((ip, port))

    def peer_count(self):
        """Return number of active peers."""
        with self._lock:
            return len(self._peers)


# Singleton tracker shared across all threads
tracker = PeerTracker()

# Round-robin state
_rr_counters = {}
_rr_lock = threading.Lock()


# ---------------------------------------------------------------
# Non-blocking forwarding
# ---------------------------------------------------------------

def forward_request(host, port, request):
    """Forward an HTTP request to a backend (non-blocking).

    Rewrites the ``Host`` header so the backend sees the
    correct address rather than the proxy's virtual hostname.

    :param host: Backend IP address.
    :param port: Backend port number.
    :param request: Raw HTTP request string.
    :rtype: bytes — raw HTTP response.
    """
    # Rewrite Host header to match actual backend
    import re as _re
    new_host = "{}:{}".format(host, port)
    request = _re.sub(
        r'(?i)^(Host:\s*)[^\r\n]*',
        r'\g<1>' + new_host,
        request,
        count=1,
        flags=_re.MULTILINE,
    )

    backend = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )

    try:
        print(
            "[Proxy] forward connecting "
            "to {}:{}".format(host, port)
        )
        # Non-blocking connect
        nb_connect(backend, (host, port), timeout=5)

        # Non-blocking send
        nb_sendall(
            backend, request.encode("utf-8"), timeout=5
        )

        # Non-blocking receive
        response = nb_recv_all(backend, timeout=5)
        if response:
            print(
                "[Proxy] forward got {} bytes "
                "from {}:{}".format(
                    len(response), host, port
                )
            )
        else:
            print(
                "[Proxy] forward empty response "
                "from {}:{}".format(host, port)
            )
        return response if response else _build_502()

    except BlockingIOError:
        print(
            "[Proxy] forward BlockingIOError"
            " to {}:{}".format(host, port)
        )
        return _build_502()
    except (socket.timeout, socket.error) as exc:
        print(
            "[Proxy] forward error to "
            "{}:{}: {}".format(host, port, exc)
        )
        return _build_502()
    finally:
        backend.close()


# ---------------------------------------------------------------
# Routing policy
# ---------------------------------------------------------------

def resolve_routing_policy(hostname, routes):
    """Decide how to route a request for *hostname*.

    Returns ``("__TRACKER__", "0")`` for Tracker queries.

    :param hostname: The Host header value.
    :param routes: Mapping produced by load_proxy_config.
    :rtype: tuple[str, str]
    """
    global _rr_counters

    if hostname == "tracker":
        return ("__TRACKER__", "0")

    route_entry = routes.get(hostname)
    if route_entry is None and ":" in hostname:
        route_entry = routes.get(hostname.split(":")[0])

    if route_entry is None:
        print(
            "[Proxy] No route for '{}'"
            " — fallback".format(hostname)
        )
        return ("127.0.0.1", "9000")

    proxy_map, policy = route_entry
    print(
        "[Proxy] resolve '{}' targets={} "
        "policy={}".format(hostname, proxy_map, policy)
    )

    proxy_host = ""
    proxy_port = "9000"

    if isinstance(proxy_map, list):
        if len(proxy_map) == 0:
            print(
                "[Proxy] Empty proxy_map "
                "for '{}'".format(hostname)
            )
            proxy_host = "127.0.0.1"
            proxy_port = "9000"

        elif len(proxy_map) == 1:
            proxy_host, proxy_port = (
                proxy_map[0].split(":", 1)
            )

        else:
            # Round-robin selection
            if policy == "round-robin":
                with _rr_lock:
                    idx = _rr_counters.get(hostname, 0)
                    selected = proxy_map[
                        idx % len(proxy_map)
                    ]
                    _rr_counters[hostname] = idx + 1
                proxy_host, proxy_port = (
                    selected.split(":", 1)
                )
                print(
                    "[Proxy] RR #{} -> {}".format(
                        idx, selected
                    )
                )
            else:
                proxy_host, proxy_port = (
                    proxy_map[0].split(":", 1)
                )
    else:
        proxy_host, proxy_port = (
            proxy_map.split(":", 1)
        )

    return proxy_host, proxy_port


# ---------------------------------------------------------------
# HTTP response builders
# ---------------------------------------------------------------

def _build_tracker_response():
    """Build JSON response with current peer list."""
    peers = tracker.get_peers()
    body = json.dumps(
        {"peers": peers, "count": len(peers)}, indent=2
    )
    body_bytes = body.encode("utf-8")
    header = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).format(len(body_bytes))
    return header.encode("utf-8") + body_bytes


def _build_json_ok(data_dict):
    """Build a 200 OK JSON response."""
    body = json.dumps(data_dict).encode("utf-8")
    header = (
        "HTTP/1.1 200 OK\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).format(len(body))
    return header.encode("utf-8") + body


def _build_400(error_msg):
    """Build a 400 Bad Request JSON response."""
    body = json.dumps(
        {"error": error_msg}
    ).encode("utf-8")
    header = (
        "HTTP/1.1 400 Bad Request\r\n"
        "Content-Type: application/json\r\n"
        "Content-Length: {}\r\n"
        "Connection: close\r\n"
        "\r\n"
    ).format(len(body))
    return header.encode("utf-8") + body


def _build_404():
    """Build a 404 Not Found response."""
    return (
        "HTTP/1.1 404 Not Found\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 13\r\n"
        "Connection: close\r\n"
        "\r\n"
        "404 Not Found"
    ).encode("utf-8")


def _build_502():
    """Build a 502 Bad Gateway response."""
    return (
        "HTTP/1.1 502 Bad Gateway\r\n"
        "Content-Type: text/plain\r\n"
        "Content-Length: 15\r\n"
        "Connection: close\r\n"
        "\r\n"
        "502 Bad Gateway"
    ).encode("utf-8")


# ---------------------------------------------------------------
# Tracker API
# ---------------------------------------------------------------

def _handle_tracker_api(request_text, addr):
    """Process Tracker REST endpoints.

    Supported:
        GET  /peers
        POST /peers/register
        POST /peers/unregister

    :param request_text: Full raw HTTP request.
    :param addr: Client address tuple.
    :rtype: bytes
    """
    lines = request_text.splitlines()
    if not lines:
        return _build_404()

    parts = lines[0].split()
    if len(parts) < 2:
        return _build_404()

    method = parts[0].upper()
    path = parts[1]

    # GET /peers
    if method == "GET" and path == "/peers":
        return _build_tracker_response()

    # POST /peers/register
    if method == "POST" and path == "/peers/register":
        body = _extract_body(request_text)
        try:
            data = json.loads(body)
            peer_ip = data.get("ip", addr[0])
            peer_port = int(data.get("port", addr[1]))
            username = data.get(
                "username", "anonymous"
            )
            tracker.register(
                peer_ip, peer_port, username
            )
            return _build_json_ok({
                "status": "registered",
                "ip": peer_ip,
                "port": peer_port,
                "username": username,
            })
        except (json.JSONDecodeError, ValueError,
                TypeError) as exc:
            return _build_400(str(exc))

    # POST /peers/unregister
    if method == "POST" and path == "/peers/unregister":
        body = _extract_body(request_text)
        try:
            data = json.loads(body)
            peer_ip = data.get("ip", addr[0])
            peer_port = int(data.get("port", addr[1]))
            removed = tracker.unregister(
                peer_ip, peer_port
            )
            status = (
                "unregistered" if removed else "not_found"
            )
            return _build_json_ok({
                "status": status,
                "ip": peer_ip,
                "port": peer_port,
            })
        except (json.JSONDecodeError, ValueError,
                TypeError) as exc:
            return _build_400(str(exc))

    return _build_404()


def _extract_body(request_text):
    """Extract the body from a raw HTTP request.

    :param request_text: Full HTTP request string.
    :rtype: str
    """
    sep = request_text.find("\r\n\r\n")
    if sep != -1:
        return request_text[sep + 4:]
    return ""


# ---------------------------------------------------------------
# Client handler (non-blocking, runs in a thread)
# ---------------------------------------------------------------

def handle_client(ip, port, conn, addr, routes):
    """Handle a single client connection (non-blocking).

    The client socket has already been set to non-blocking
    by :func:`nb_accept`.  All recv/send use
    :func:`nb_recv_all` / :func:`nb_sendall`.

    :param ip: Proxy server IP.
    :param port: Proxy server port.
    :param conn: Client socket (non-blocking).
    :param addr: Client address tuple.
    :param routes: Hostname → backend mapping.
    """
    try:
        conn.setblocking(False)

        # Non-blocking receive
        raw = nb_recv_all(conn, timeout=5, bufsize=4096)
        if not raw:
            return

        request = raw.decode("utf-8", errors="replace")
        if not request.strip():
            return

        # Extract Host header
        hostname = None
        request_path = "/"
        for line in request.splitlines():
            if line.lower().startswith("host:"):
                hostname = line.split(":", 1)[1].strip()
                break

        # Extract path from request line
        req_parts = request.splitlines()[0].split()
        if len(req_parts) >= 2:
            request_path = req_parts[1]

        if not hostname:
            print(
                "[Proxy] No Host header from "
                "{}".format(addr)
            )
            nb_sendall(conn, _build_404(), timeout=5)
            return

        print(
            "[Proxy] {} Host: {} "
            "Path: {}".format(addr, hostname, request_path)
        )

        # Tracker API (path-based)
        if request_path.startswith("/peers"):
            print(
                "[Proxy] Tracker API {} -> "
                "{}".format(addr, request_path)
            )
            response = _handle_tracker_api(request, addr)
            nb_sendall(conn, response, timeout=5)
            return

        # Resolve routing
        resolved_host, resolved_port = (
            resolve_routing_policy(hostname, routes)
        )

        # Tracker sentinel
        if resolved_host == "__TRACKER__":
            response = _handle_tracker_api(request, addr)
            nb_sendall(conn, response, timeout=5)
            return

        # Validate port
        try:
            resolved_port = int(resolved_port)
        except ValueError:
            print(
                "[Proxy] Bad port '{}' for "
                "'{}'".format(resolved_port, hostname)
            )
            nb_sendall(conn, _build_404(), timeout=5)
            return

        # Forward or 404
        if resolved_host:
            print(
                "[Proxy] Forward {} -> {}:{}".format(
                    hostname, resolved_host, resolved_port
                )
            )
            response = forward_request(
                resolved_host, resolved_port, request
            )
        else:
            response = _build_404()

        nb_sendall(conn, response, timeout=5)

    except BlockingIOError:
        print(
            "[Proxy] BlockingIOError "
            "from {}".format(addr)
        )
    except (socket.error, OSError) as exc:
        print(
            "[Proxy] handle_client error: "
            "{}".format(exc)
        )
    except Exception as exc:
        print(
            "[Proxy] handle_client exception: "
            "{}".format(exc)
        )
    finally:
        try:
            conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------
# Server loop (non-blocking accept)
# ---------------------------------------------------------------

def run_proxy(ip, port, routes):
    """Start the proxy server with non-blocking accept loop.

    The listening socket is set to ``setblocking(False)``.
    Connections are accepted via :func:`nb_accept`
    (``select``-based) and dispatched to daemon threads.

    :param ip: IP to bind.
    :param port: Port to listen on.
    :param routes: Hostname → backend mapping.
    """
    proxy = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM
    )
    proxy.setsockopt(
        socket.SOL_SOCKET, socket.SO_REUSEADDR, 1
    )

    try:
        proxy.bind((ip, port))
        proxy.listen(50)
        proxy.setblocking(False)

        print(
            "[Proxy] Listening {}:{} "
            "(non-blocking)".format(ip, port)
        )
        print(
            "[Proxy] Tracker: {} peer(s)".format(
                tracker.peer_count()
            )
        )

        while True:
            # Non-blocking accept with select
            conn, addr = nb_accept(proxy, timeout=1.0)
            if conn is None:
                continue

            print(
                "[Proxy] Accepted from {}".format(addr)
            )

            client_thread = threading.Thread(
                target=handle_client,
                args=(ip, port, conn, addr, routes),
                daemon=True,
            )
            client_thread.start()

    except KeyboardInterrupt:
        print("[Proxy] Shutting down...")
    except socket.error as exc:
        print("[Proxy] Socket error: {}".format(exc))
    finally:
        proxy.close()


def create_proxy(ip, port, routes=None):
    """Entry point for launching the proxy server.

    :param ip: IP to bind.
    :param port: Port to listen on.
    :param routes: Hostname mapping (auto-loads config
        if None).
    """
    if not routes:
        routes = load_proxy_config("config/proxy.conf")

    run_proxy(ip, port, routes)
