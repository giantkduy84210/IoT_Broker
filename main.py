import threading
import time
import json
import paho.mqtt.client as mqtt

from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from fastapi import WebSocket
from typing import List
import asyncio

import uvicorn

from mqtt_state import mqtt_messages, connected_devices, device_last_seen, device_status, OFFLINE_THRESHOLD

# =========================
# CONFIG
# =========================
MQTT_BROKER = "127.0.0.1"
MQTT_PORT = 1883

THINGSBOARD_HOST = "app.coreiot.io"
THINGSBOARD_PORT = 1883
ACCESS_TOKEN = "oynsH35Xdan1UnQ2aNH6"

# =========================
# FASTAPI
# =========================
class WSManager:
    def __init__(self):
        self.clients: List[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.clients.append(ws)

    def disconnect(self, ws: WebSocket):
        self.clients.remove(ws)

    async def broadcast(self, data: dict):
        dead = []
        for c in self.clients:
            try:
                await c.send_json(data)
            except:
                dead.append(c)
        for d in dead:
            self.clients.remove(d)

ws_manager = WSManager()

app = FastAPI()

# 1. API ROUTES (PHẢI ĐẶT TRƯỚC)
@app.get("/api/devices")
def get_devices():
    return {"devices": list(connected_devices)}

@app.get("/api/messages")
def get_messages():
    return {"messages": mqtt_messages[-50:]}

# 2. SERVE STATIC SAU
app.mount("/static", StaticFiles(directory="static"), name="static")

# 3. ROOT HTML
@app.get("/")
def index():
    return FileResponse("static/index.html")

@app.websocket("/ws")
async def ws_endpoint(websocket: WebSocket):
    await ws_manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()  # keep alive
    except:
        ws_manager.disconnect(websocket)


def push_ws(data):
    asyncio.run(ws_manager.broadcast(data))


def update_device_status(device, ts):
    prev = device_status.get(device)
    device_last_seen[device] = ts
    device_status[device] = "ONLINE"
    return prev != "ONLINE"

# =========================
# COREIOT CLIENT
# =========================
def on_coreiot_connect(client, userdata, flags, rc):
    # print("CoreIoT connected:", rc)
    client.subscribe("v1/gateway/rpc")

def on_coreiot_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        device = payload.get("device")
        data = payload.get("data", {})

        # print("CoreIoT message received: ", payload) 

        if not device:
            return

        ts = time.time()

        topic = f"devices/{device}/rpc"
        rpc_payload = {
            "id": data.get("id"),
            "method": data.get("method"),
            "params": data.get("params")
        }
        # print("Publishing RPC to local MQTT:", topic, rpc_payload)
        local_mqtt.publish(topic, json.dumps(rpc_payload))

        push_ws({
            "type": "rpc",
            "device": device,
            "payload": rpc_payload,
            "ts": ts
        })

    except Exception as e:
        print("RPC error:", e)

coreiot_client = mqtt.Client("bridge")
coreiot_client.username_pw_set(ACCESS_TOKEN)
coreiot_client.on_connect = on_coreiot_connect
coreiot_client.on_message = on_coreiot_message
coreiot_client.connect(THINGSBOARD_HOST, THINGSBOARD_PORT, 60)
coreiot_client.loop_start()

# =========================
# LOCAL MQTT
# =========================
def on_local_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode().strip())

        device = data.get("device")
        if not device:
            return

        ts = time.time()

        # update last seen / status
        changed = update_device_status(device, ts)

        mqtt_messages.append({
            "topic": msg.topic,
            "payload": data,
            "ts": ts
        })

        connected_devices.add(device)

        gateway_payload = {
            device: [{
                "ts": int(ts * 1000),
                "values": data
            }]
        }

        coreiot_client.publish(
            "v1/gateway/telemetry",
            json.dumps(gateway_payload)
        )
        

        # heartbeat support
        if data.get("type") == "heartbeat":
            push_ws({
                "type": "device_status",
                "device": device,
                "status": "ONLINE",
                "ts": ts
            })
            return

        # notify frontend when device first appears or status changed
        if changed:
            push_ws({
                "type": "device_status",
                "device": device,
                "status": "ONLINE",
                "ts": ts
            })

        push_ws({
            "type": "telemetry",
            "device": device,
            "payload": data,
            "ts": ts
        })
        

    except Exception as e:
        print("local error:", e)

local_mqtt = mqtt.Client("local-bridge")
local_mqtt.on_message = on_local_message

def start_mqtt():
    local_mqtt.connect(MQTT_BROKER, MQTT_PORT, 60)
    local_mqtt.subscribe("devices/+/telemetry")
    local_mqtt.loop_forever()

# =========================
# WEB
# =========================
def start_web():
    uvicorn.run(app, host="0.0.0.0", port=8000)

# =========================
# MAIN
# =========================
if __name__ == "__main__":
    threading.Thread(target=start_mqtt, daemon=True).start()
    threading.Thread(target=start_web, daemon=True).start()

    def monitor_devices():
        while True:
            now = time.time()

            for device, last in list(device_last_seen.items()):
                if now - last > OFFLINE_THRESHOLD:
                    if device_status.get(device) != "OFFLINE":
                        device_status[device] = "OFFLINE"

                        push_ws({
                            "type": "device_status",
                            "device": device,
                            "status": "OFFLINE",
                            "ts": now
                        })

            time.sleep(5)

    threading.Thread(target=monitor_devices, daemon=True).start()

    while True:
        time.sleep(1)