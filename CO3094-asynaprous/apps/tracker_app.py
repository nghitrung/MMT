import json
from daemon import AsynapRous

app = AsynapRous()

# Bộ nhớ tạm lưu danh sách peers (Tracker)
active_peers = {}

@app.route('/login', methods=['POST'])
async def login(headers="guest", body="anonymous"):
    # Trình duyệt gọi vào đây với username/password
    user_info = json.loads(body)
    username = user_info.get("username")
    password = user_info.get("password")
    
    # Xác thực logic ở đây
    return json.dumps({"status": "success", "username": username}).encode("utf-8")

@app.route('/peers', methods=['POST'])
async def register_peer(headers="guest", body="anonymous"):
    # Đăng ký Local Server của Client vào Tracker
    peer_info = json.loads(body)
    username = peer_info.get("username")
    active_peers[username] = {
        "ip": peer_info.get("ip"),
        "port": peer_info.get("port"),
        "status": "online"
    }
    return json.dumps({"status": "registered"}).encode("utf-8")

@app.route('/peers', methods=['GET'])
async def get_peers(headers="guest", body="anonymous"):
    # Trả về danh sách peers cho client
    return json.dumps({"peers": active_peers}).encode("utf-8")