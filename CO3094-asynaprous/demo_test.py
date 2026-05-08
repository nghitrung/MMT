"""Quick demo: test all features of the AsynapRous system."""
import socket
import json
import time


def send_request(host, port, method, path, body_dict=None):
    """Send a raw HTTP request and return the response body."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    try:
        sock.connect((host, port))
        body_bytes = b""
        if body_dict:
            body_bytes = json.dumps(body_dict).encode("utf-8")

        request = (
            "{} {} HTTP/1.1\r\n"
            "Host: tracker\r\n"
            "Content-Type: application/json\r\n"
            "Content-Length: {}\r\n"
            "Connection: close\r\n"
            "\r\n"
        ).format(method, path, len(body_bytes))

        sock.sendall(request.encode("utf-8") + body_bytes)

        resp = b""
        while True:
            try:
                chunk = sock.recv(4096)
                if not chunk:
                    break
                resp += chunk
            except socket.timeout:
                break

        decoded = resp.decode("utf-8", errors="replace")
        parts = decoded.split("\r\n\r\n", 1)
        return parts[1] if len(parts) > 1 else decoded

    finally:
        sock.close()


PROXY = ("127.0.0.1", 8080)
BACKEND = ("127.0.0.1", 9001)

print("=" * 60)
print("  AsynapRous Demo Test")
print("=" * 60)

# 1. Register alice on Tracker
print("\n[1] POST /peers/register (alice)")
resp = send_request(*PROXY, "POST", "/peers/register",
                     {"ip": "127.0.0.1", "port": 9001,
                      "username": "alice"})
print("    Response:", resp)

# 2. Register bob on Tracker
print("\n[2] POST /peers/register (bob)")
resp = send_request(*PROXY, "POST", "/peers/register",
                     {"ip": "127.0.0.1", "port": 9002,
                      "username": "bob"})
print("    Response:", resp)

# 3. Get peer list
print("\n[3] GET /peers")
resp = send_request(*PROXY, "GET", "/peers")
print("    Response:", resp)

# 4. Send echo to backend
print("\n[4] POST /echo (to backend:9001)")
resp = send_request(*BACKEND, "POST", "/echo",
                     {"hello": "world"})
print("    Response:", resp)

# 5. Send P2P message to backend
print("\n[5] POST /message (P2P to backend:9001)")
resp = send_request(*BACKEND, "POST", "/message",
                     {"from": "bob", "message": "Hey Alice!"})
print("    Response:", resp)

# 6. Unregister bob
print("\n[6] POST /peers/unregister (bob)")
resp = send_request(*PROXY, "POST", "/peers/unregister",
                     {"ip": "127.0.0.1", "port": 9002})
print("    Response:", resp)

# 7. Check peers again
print("\n[7] GET /peers (after unregister)")
resp = send_request(*PROXY, "GET", "/peers")
print("    Response:", resp)

print("\n" + "=" * 60)
print("  Demo complete!")
print("=" * 60)
