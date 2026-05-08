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
daemon.httpadapter
~~~~~~~~~~~~~~~~~

This module provides a http adapter object to manage and
persist http settings (headers, bodies). The adapter supports
both raw URL paths and RESTful route definitions, and
integrates with Request and Response objects to handle
client-server communication.

All socket I/O uses ``setblocking(False)`` with explicit
``BlockingIOError`` handling via :mod:`daemon.utils`.
"""

from .request import Request
from .response import Response
from .dictionary import CaseInsensitiveDict
from .utils import nb_recv_all, nb_sendall

import asyncio
import inspect
import json
import datetime


class HttpAdapter:
    """Adapter for managing client connections and routing
    requests.

    The ``HttpAdapter`` encapsulates the logic for receiving
    HTTP requests, dispatching them to appropriate route
    handlers, and constructing responses.  It supports RESTful
    routing via hooks and integrates with
    :class:`Request` and :class:`Response` objects for full
    request lifecycle management.

    Attributes:
        ip (str): IP address of the client.
        port (int): Port number of the client.
        conn (socket): Active socket connection.
        connaddr (tuple): Address of the connected client.
        routes (dict): Route paths mapped to handler functions.
        request (Request): Object for parsing incoming data.
        response (Response): Object for building replies.
    """

    __attrs__ = [
        "ip",
        "port",
        "conn",
        "connaddr",
        "routes",
        "request",
        "response",
    ]

    def __init__(self, ip, port, conn, connaddr, routes):
        """Initialise a new HttpAdapter instance.

        :param ip: IP address of the client.
        :param port: Port number of the client.
        :param conn: Active socket connection.
        :param connaddr: Address of the connected client.
        :param routes: Mapping of route paths to handlers.
        """
        #: IP address.
        self.ip = ip
        #: Port.
        self.port = port
        #: Connection
        self.conn = conn
        #: Connection address
        self.connaddr = connaddr
        #: Routes
        self.routes = routes if routes else {}
        #: Request
        self.request = Request()
        #: Response
        self.response = Response()

    # ----------------------------------------------------------
    # Synchronous handler (non-blocking socket)
    # ----------------------------------------------------------

    def handle_client(self, conn, addr, routes):
        """Handle an incoming client connection.

        The socket is set to **non-blocking** mode.  Reads and
        writes use :func:`nb_recv_all` / :func:`nb_sendall`
        which handle ``BlockingIOError`` internally.

        :param conn: The client socket connection.
        :param addr: The client's address.
        :param routes: The route mapping for dispatching.
        """
        # Store connection state
        self.conn = conn
        self.connaddr = addr
        req = self.request
        resp = self.response

        # --- Non-blocking receive ---
        conn.setblocking(False)
        raw = nb_recv_all(conn, timeout=5, bufsize=4096)

        if not raw:
            conn.close()
            return

        msg = raw.decode("utf-8", errors="replace")

        # Guard: empty request
        if not msg.strip():
            conn.close()
            return

        req.prepare(msg, routes)
        print(
            "[HttpAdapter] Invoke handle_client "
            "connection {}".format(addr)
        )

        # --- Dispatch to hook or static file ---
        if req.hook:
            response = self._invoke_hook(req, msg)
        else:
            response = resp.build_response(req)

        # --- Non-blocking send ---
        try:
            nb_sendall(conn, response, timeout=5)
        except (BlockingIOError, OSError) as exc:
            print(
                "[HttpAdapter] send error: {}".format(exc)
            )
        finally:
            conn.close()

    # ----------------------------------------------------------
    # Asynchronous handler (asyncio StreamReader / StreamWriter)
    # ----------------------------------------------------------

    async def handle_client_coroutine(self, reader, writer):
        """Handle a client connection asynchronously.

        Uses ``asyncio.StreamReader`` / ``StreamWriter`` for
        non-blocking I/O — no raw socket calls needed.

        :param reader: The async stream reader.
        :param writer: The async stream writer.
        """
        req = self.request
        resp = self.response

        addr = writer.get_extra_info("peername")
        print(
            "[HttpAdapter] Invoke handle_client_coroutine "
            "connection {})".format(addr)
        )

        # Async non-blocking read
        msg = await reader.read(4096)

        if not msg or not msg.strip():
            writer.close()
            await writer.wait_closed()
            return

        decoded_msg = msg.decode("utf-8", errors="replace")
        req.prepare(decoded_msg, routes=self.routes)

        # Dispatch
        if req.hook:
            response = await self._invoke_hook_async(
                req, decoded_msg
            )
        else:
            response = resp.build_response(req)

        # Async non-blocking write
        writer.write(response)
        await writer.drain()

    # ----------------------------------------------------------
    # Hook invocation helpers
    # ----------------------------------------------------------

    def _invoke_hook(self, req, raw_msg):
        """Invoke a route handler synchronously.

        If the hook is a coroutine function it is executed
        inside a temporary event loop.

        :param req: Prepared Request object (with hook set).
        :param raw_msg: Raw HTTP message string.
        :rtype: bytes — complete HTTP response.
        """
        raw_headers, raw_body = req.fetch_headers_body(
            raw_msg
        )
        headers = req.prepare_headers(raw_msg)

        try:
            if inspect.iscoroutinefunction(req.hook):
                loop = asyncio.new_event_loop()
                try:
                    hook_result = loop.run_until_complete(
                        req.hook(headers, raw_body)
                    )
                finally:
                    loop.close()
            else:
                hook_result = req.hook(headers, raw_body)
        except Exception as exc:
            print(
                "[HttpAdapter] Hook exception: "
                "{}".format(exc)
            )
            hook_result = json.dumps(
                {"error": str(exc)}
            ).encode("utf-8")

        return self._wrap_hook_result(hook_result)

    async def _invoke_hook_async(self, req, raw_msg):
        """Invoke a route handler inside an async context.

        :param req: Prepared Request object.
        :param raw_msg: Raw HTTP message string.
        :rtype: bytes
        """
        raw_headers, raw_body = req.fetch_headers_body(
            raw_msg
        )
        headers = req.prepare_headers(raw_msg)

        try:
            if inspect.iscoroutinefunction(req.hook):
                hook_result = await req.hook(
                    headers, raw_body
                )
            else:
                hook_result = req.hook(headers, raw_body)
        except Exception as exc:
            print(
                "[HttpAdapter] Async hook exception: "
                "{}".format(exc)
            )
            hook_result = json.dumps(
                {"error": str(exc)}
            ).encode("utf-8")

        return self._wrap_hook_result(hook_result)

    @staticmethod
    def _wrap_hook_result(hook_result):
        """Normalise hook output and wrap in an HTTP envelope.

        :param hook_result: bytes, str, or None.
        :rtype: bytes
        """
        if isinstance(hook_result, str):
            hook_result = hook_result.encode("utf-8")
        elif hook_result is None:
            hook_result = b""

        return HttpAdapter._build_hook_response(
            hook_result, status_code=200
        )

    # ----------------------------------------------------------
    # HTTP response builders
    # ----------------------------------------------------------

    @staticmethod
    def _build_hook_response(
        body_bytes,
        status_code=200,
        content_type="application/json",
    ):
        """Build a complete HTTP/1.1 response from body bytes.

        :param body_bytes: Response body content.
        :param status_code: HTTP status code (default 200).
        :param content_type: Content-Type header value.
        :rtype: bytes
        """
        reason_phrases = {
            200: "OK",
            201: "Created",
            204: "No Content",
            400: "Bad Request",
            404: "Not Found",
            500: "Internal Server Error",
        }
        reason = reason_phrases.get(status_code, "OK")
        content_length = len(body_bytes)
        now = datetime.datetime.utcnow().strftime(
            "%a, %d %b %Y %H:%M:%S GMT"
        )

        header = (
            "HTTP/1.1 {} {}\r\n"
            "Content-Type: {}\r\n"
            "Content-Length: {}\r\n"
            "Date: {}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).format(
            status_code, reason,
            content_type, content_length, now,
        )

        return header.encode("utf-8") + body_bytes

    # ----------------------------------------------------------
    # Cookie extraction
    # ----------------------------------------------------------

    def extract_cookies(self, req, resp):
        """Parse cookies from request headers.

        :param req: The Request object.
        :param resp: The Response object.
        :rtype: dict — cookie key-value pairs.
        """
        cookies = {}
        raw_headers = req.headers if req.headers else {}
        cookie_str = ""

        if isinstance(raw_headers, dict):
            cookie_str = raw_headers.get(
                "cookie", raw_headers.get("Cookie", "")
            )
        elif isinstance(raw_headers, str):
            for line in raw_headers.splitlines():
                if line.lower().startswith("cookie:"):
                    cookie_str = line.split(":", 1)[1].strip()
                    break

        if cookie_str:
            for pair in cookie_str.split(";"):
                pair = pair.strip()
                if "=" in pair:
                    key, value = pair.split("=", 1)
                    cookies[key.strip()] = value.strip()

        return cookies

    # ----------------------------------------------------------
    # High-level response builders
    # ----------------------------------------------------------

    def build_response(self, req, resp):
        """Build a Response object from raw response data.

        :param req: The Request used to generate the response.
        :param resp: The raw response object.
        :rtype: Response
        """
        response = Response()
        response.encoding = self._get_encoding_from_headers(
            response.headers
        )
        response.raw = resp
        response.reason = getattr(resp, "reason", None)

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        response.cookies = self.extract_cookies(req, resp)
        response.request = req
        response.connection = self
        return response

    def build_json_response(self, req, resp):
        """Build a Response object from JSON data.

        :param req: The Request used to generate the response.
        :param resp: The raw response object.
        :rtype: Response
        """
        response = Response(req)
        response.raw = resp

        if isinstance(req.url, bytes):
            response.url = req.url.decode("utf-8")
        else:
            response.url = req.url

        response.request = req
        response.connection = self
        return response

    def _get_encoding_from_headers(self, headers):
        """Extract charset from Content-Type header.

        :param headers: Response headers dictionary.
        :rtype: str or None
        """
        if not headers:
            return None

        content_type = None
        if isinstance(headers, dict):
            content_type = headers.get(
                "Content-Type",
                headers.get("content-type", ""),
            )
        elif hasattr(headers, "__getitem__"):
            try:
                content_type = headers["Content-Type"]
            except (KeyError, TypeError):
                return None

        if not content_type:
            return None

        for part in content_type.split(";"):
            part = part.strip()
            if part.lower().startswith("charset="):
                return part.split("=", 1)[1].strip()

        return None

    # def get_connection(self, url, proxies=None):
        # """Returns a url connection for the given URL.
        # :param url: The URL to connect to.
        # :param proxies: (optional) Proxies dict.
        # :rtype: int
        # """
        # pass

    def add_headers(self, request):
        """Add headers to the request (override in subclass).

        :param request: Request to add headers to.
        """
        pass

    def build_proxy_headers(self, proxy):
        """Build headers for proxied requests.

        :param proxy: The proxy URL.
        :rtype: dict
        """
        headers = {}
        #
        # TODO: build your authentication here
        #       username, password =...
        # we provide dummy auth here
        #
        username, password = ("user1", "password")

        if username:
            headers["Proxy-Authorization"] = (
                username, password
            )

        return headers