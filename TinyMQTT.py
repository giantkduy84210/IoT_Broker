import asyncio
import threading
import time
import json
import paho.mqtt.client as mqtt

from hbmqtt.broker import Broker
from hbmqtt.mqtt.protocol.handler import EVENT_MQTT_PACKET_SENT, ProtocolHandler

# =========================
# GLOBAL SYNC FLAGS
# =========================
broker_ready = threading.Event()
local_mqtt = None

# =========================
# PATCH HBMQTT (stability)
# =========================
async def _patched_send_packet(self, packet):
    try:
        await self._write_lock.acquire()
        try:
            await packet.to_stream(self.writer)
        finally:
            self._write_lock.release()

        if self._keepalive_task:
            self._keepalive_task.cancel()
            self._keepalive_task = self._loop.call_later(
                self.keepalive_timeout,
                self.handle_write_timeout
            )

        await self.plugins_manager.fire_event(
            EVENT_MQTT_PACKET_SENT,
            packet=packet,
            session=self.session
        )

    except ConnectionResetError:
        await self.handle_connection_closed()
        raise
    except BaseException as exc:
        self.logger.warning("Unhandled exception: %s", exc)
        raise

ProtocolHandler._send_packet = _patched_send_packet

# =========================
# COREIOT CONFIG
# =========================
THINGSBOARD_HOST = "app.coreiot.io"
THINGSBOARD_PORT = 1883
ACCESS_TOKEN = "oynsH35Xdan1UnQ2aNH6"

# =========================
# CONNECT DEVICE (IMPORTANT)
# =========================
def connect_device(device_name):
    payload = {"device": device_name}
    coreiot_client.publish(
        "v1/gateway/connect",
        json.dumps(payload)
    )
    print(f"Device connected → {device_name}")

# =========================
# RPC FROM COREIOT
# =========================
def on_coreiot_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print("📩 RPC RAW:", payload)

        device = payload.get("device")
        req_id = payload.get("id")
        data = payload.get("data", {})

        if not device:
            print("❌ Missing device in RPC")
            return

        rpc_payload = {
            "id": req_id,
            "method": data.get("method"),
            "params": data.get("params")
        }

        topic = f"devices/{device}/rpc"

        if local_mqtt:
            local_mqtt.publish(topic, json.dumps(rpc_payload))
            print(f"➡️ RPC forwarded → {topic}")
        else:
            print("❌ local_mqtt not ready")

    except Exception as e:
        print("RPC error:", e)

def on_coreiot_connect(client, userdata, flags, rc):
    print("Connected rc =", rc)
    if rc == 0:
        result, mid = client.subscribe("v1/gateway/rpc")
        connect_device("ESP32_001")
        connect_device("ESP32_002")
        print("SUBSCRIBE RESULT:", result, mid)
    else:
        print("Failed to connect to CoreIoT, rc =", rc)

coreiot_client = mqtt.Client("BridgeClient", protocol=mqtt.MQTTv311)
coreiot_client.username_pw_set(ACCESS_TOKEN)
coreiot_client.on_connect = on_coreiot_connect
coreiot_client.on_message = on_coreiot_message
coreiot_client.connect(THINGSBOARD_HOST, THINGSBOARD_PORT, 60)
coreiot_client.loop_start()

# =========================
# MQTT BROKER CONFIG
# =========================
broker_config = {
    "listeners": {
        "default": {
            "type": "tcp",
            "bind": "0.0.0.0:1884"
        }
    },
    "sys_interval": 10,
    "auth": {
        "allow-anonymous": True
    },
    "topic-check": {
        "enabled": True,
        "plugins": ["topic_taboo"]
    }
}

def start_broker():
    async def broker_coro():
        broker = Broker(broker_config)
        await broker.start()
        print("🚀 MQTT Broker started")
        broker_ready.set()

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.run_until_complete(broker_coro())
    loop.run_forever()

# =========================
# LOCAL SUBSCRIBER (ESP SIM)
# =========================
def run_subscriber():
    def on_message(client, userdata, msg):
        if msg.topic.startswith("$SYS/"):
            return

        try:
            data = json.loads(msg.payload.decode())

            device = data.get("device")
            temp = data.get("temperature")
            hum = data.get("humidity")
            alert_status = data.get("alert_status")

            if not device:
                return

            gateway_payload = {
                device: [
                    {
                        "ts": int(time.time() * 1000),
                        "values": {
                            "temperature": temp,
                            "humidity": hum,
                            "alert_status": alert_status
                        }
                    }
                ]
            }

            coreiot_client.publish(
                "v1/gateway/telemetry",
                json.dumps(gateway_payload)
            )

            print("📤 telemetry → CoreIoT:", gateway_payload)

        except Exception as e:
            print("Subscriber error:", e)

    client = mqtt.Client("LocalSubscriber", protocol=mqtt.MQTTv311)
    client.on_message = on_message

    broker_ready.wait()
    client.connect("127.0.0.1", 1884)
    client.subscribe("#")
    client.loop_forever()

# =========================
# MAIN
# =========================
if __name__ == "__main__":

    # start broker
    threading.Thread(target=start_broker, daemon=True).start()
    broker_ready.wait()

    # local bridge mqtt (for RPC routing)
    local_mqtt = mqtt.Client("LocalBridge", protocol=mqtt.MQTTv311)
    local_mqtt.connect("127.0.0.1", 1884)
    local_mqtt.loop_start()

    # start subscriber
    threading.Thread(target=run_subscriber, daemon=True).start()

    # keep alive
    while True:
        time.sleep(1)