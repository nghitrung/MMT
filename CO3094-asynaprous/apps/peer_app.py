import json
import socket
from daemon import AsynapRous

app = AsynapRous()

@app.route('/send-peer', methods=['POST'])
async def send_to_peer(headers="guest", body="anonymous"):
    # Trình duyệt của client gọi API này để ra lệnh gửi tin nhắn
    req_data = json.loads(body)
    target_ip = req_data.get("target_ip")
    target_port = int(req_data.get("target_port"))
    message = req_data.get("message")

    # Local Server mở kết nối TCP trực tiếp tới Client 2 (P2P)
    try:
        peer_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        peer_sock.connect((target_ip, target_port))
        
        # Gói tin gửi đi
        payload = json.dumps({"from": "Client1", "content": message})
        
        # Giả lập HTTP Request đơn giản để HttpAdapter của Client 2 hiểu được
        http_req = (
            "POST /receive-msg HTTP/1.1\r\n"
            "Content-Type: application/json\r\n"
            f"Content-Length: {len(payload)}\r\n\r\n"
            f"{payload}"
        )
        peer_sock.sendall(http_req.encode())
        peer_sock.close()
        return json.dumps({"status": "sent"}).encode("utf-8")
    except Exception as e:
        return json.dumps({"error": str(e)}).encode("utf-8")

@app.route('/receive-msg', methods=['POST'])
async def receive_msg(headers="guest", body="anonymous"):
    # API này lắng nghe tin nhắn từ các Peer khác gửi tới
    msg_data = json.loads(body)
    print("\n[TIN NHẮN MỚI] Từ {}: {}".format(msg_data.get("from"), msg_data.get("content")))
    return json.dumps({"status": "received"}).encode("utf-8")