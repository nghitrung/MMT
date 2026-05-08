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
daemon.utils
~~~~~~~~~~~~~

Non-blocking socket utilities and common helpers for the
AsynapRous framework.  Every socket operation uses
``setblocking(False)`` with ``select`` for readiness
notification, and explicitly handles ``BlockingIOError``
when the OS buffer is empty or full.
"""

import socket
import select
import errno
from urllib.parse import urlparse, unquote


# ------------------------------------------------------------------
# Non-blocking connect
# ------------------------------------------------------------------

def nb_connect(sock, address, timeout=5):
    """Perform a non-blocking connect.

    Sets the socket to non-blocking, initiates the connection,
    and waits for writability via ``select``.  Raises
    ``socket.timeout`` if the connection is not established
    within *timeout* seconds.

    :param sock: An open ``socket.socket`` instance.
    :param address: ``(host, port)`` tuple.
    :param timeout: Seconds to wait (default 5).
    :raises socket.timeout: If the connection cannot be
        established in time.
    :raises socket.error: If the OS reports a connection error.
    """
    sock.setblocking(False)
    try:
        sock.connect(address)
    except BlockingIOError:
        # Connection in progress — wait for writability
        _, writable, _ = select.select([], [sock], [], timeout)
        if not writable:
            raise socket.timeout(
                "nb_connect timed out to {}".format(address)
            )
        # Check for deferred connection errors
        err = sock.getsockopt(
            socket.SOL_SOCKET, socket.SO_ERROR
        )
        if err != 0:
            raise socket.error(
                err, "nb_connect failed: {}".format(
                    errno.errorcode.get(err, err)
                )
            )
    except OSError as exc:
        # Windows may raise a generic OSError instead
        if exc.errno in (errno.EINPROGRESS, errno.EWOULDBLOCK,
                         getattr(errno, "WSAEWOULDBLOCK", 10035)):
            _, writable, _ = select.select(
                [], [sock], [], timeout
            )
            if not writable:
                raise socket.timeout(
                    "nb_connect timed out to {}".format(address)
                )
            err = sock.getsockopt(
                socket.SOL_SOCKET, socket.SO_ERROR
            )
            if err != 0:
                raise socket.error(err)
        else:
            raise


# ------------------------------------------------------------------
# Non-blocking send
# ------------------------------------------------------------------

def nb_sendall(sock, data, timeout=5):
    """Send *data* over a non-blocking socket.

    Handles ``BlockingIOError`` (buffer full) by waiting for
    the socket to become writable again via ``select``.

    :param sock: A connected, non-blocking socket.
    :param data: ``bytes`` to send.
    :param timeout: Seconds to wait per chunk (default 5).
    :raises socket.timeout: If the socket never becomes
        writable within *timeout*.
    :raises ConnectionError: If the connection is closed.
    """
    sock.setblocking(False)
    total_sent = 0
    data_len = len(data)

    while total_sent < data_len:
        try:
            sent = sock.send(data[total_sent:])
            if sent == 0:
                raise ConnectionError(
                    "nb_sendall: connection closed"
                )
            total_sent += sent
        except BlockingIOError:
            # Buffer full — wait until writable
            _, writable, _ = select.select(
                [], [sock], [], timeout
            )
            if not writable:
                raise socket.timeout(
                    "nb_sendall timed out after "
                    "{} / {} bytes".format(total_sent, data_len)
                )

    return total_sent


# ------------------------------------------------------------------
# Non-blocking recv
# ------------------------------------------------------------------

def nb_recv_all(sock, timeout=5, bufsize=4096):
    """Receive all available data until the peer closes the
    connection or *timeout* expires with no new data.

    After the first chunk arrives, subsequent chunks use a
    shorter inter-chunk timeout (0.3 s) to avoid waiting the
    full *timeout* after the last byte.

    Handles ``BlockingIOError`` (buffer empty) by waiting
    for readability via ``select``.

    :param sock: A connected, non-blocking socket.
    :param timeout: Seconds to wait for the first chunk.
    :param bufsize: Read buffer size (default 4096).
    :rtype: bytes
    """
    sock.setblocking(False)
    data = b""
    # Use full timeout for the first read, then a short
    # inter-chunk timeout once data starts flowing.
    current_timeout = timeout

    while True:
        readable, _, _ = select.select(
            [sock], [], [], current_timeout
        )
        if not readable:
            break
        try:
            chunk = sock.recv(bufsize)
            if not chunk:
                break  # Peer closed
            data += chunk
            # After first data, switch to short timeout
            current_timeout = 0.3
        except BlockingIOError:
            continue
        except ConnectionResetError:
            break

    return data



# ------------------------------------------------------------------
# Non-blocking accept (for server sockets)
# ------------------------------------------------------------------

def nb_accept(server_sock, timeout=1.0):
    """Wait for a connection on a non-blocking server socket.

    :param server_sock: A listening, non-blocking socket.
    :param timeout: Seconds to wait (default 1.0).
    :rtype: ``(conn, addr)`` or ``(None, None)`` on timeout.
    """
    server_sock.setblocking(False)
    readable, _, _ = select.select(
        [server_sock], [], [], timeout
    )
    if not readable:
        return None, None
    try:
        conn, addr = server_sock.accept()
        conn.setblocking(False)
        return conn, addr
    except BlockingIOError:
        return None, None


# ------------------------------------------------------------------
# Legacy helper (fixed for Python 3)
# ------------------------------------------------------------------

def get_auth_from_url(url):
    """Extract authentication components from a URL.

    :param url: URL string with optional ``user:pass@host``.
    :rtype: tuple[str, str]
    """
    parsed = urlparse(url)
    try:
        auth = (
            unquote(parsed.username),
            unquote(parsed.password),
        )
    except (AttributeError, TypeError):
        auth = ("", "")
    return auth