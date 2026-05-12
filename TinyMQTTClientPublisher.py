import paho.mqtt.client as mqtt
import time
import threading
import json

device_id = "ESP32_001"
broker_address = "192.168.1.4"

connected = threading.Event()


topic_rpc = f"devices/{device_id}/rpc"
topic_telemetry = f"devices/{device_id}/telemetry"

last_alert_status = None

def on_connect(client, userdata, flags, rc):
    print("Connected:", rc)
    client.subscribe(topic_rpc)
    connected.set()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        print("📩 RPC received:", payload)

        method = payload.get("method")
        params = payload.get("params")

        print(f"Method: {method}, Params: {params}")
        if method == "POWER":
            if params == "ON":
                print("🔌 Relay ON")
            elif params == "OFF":
                print("🔌 Relay OFF")

    except Exception as e:
        print("RPC parse error:", e)


def on_disconnect(client, userdata, rc):
    print(f"Disconnected with result code {rc}.")


def on_publish(client, userdata, mid):
    print(f"📨 Message ID {mid} published successfully")


client = mqtt.Client(client_id="PythonPublisher", protocol=mqtt.MQTTv311)
client.on_connect = on_connect
client.on_message = on_message
client.on_disconnect = on_disconnect
client.on_publish = on_publish
client.connect(broker_address, 1883)
client.loop_start()

if not connected.wait(timeout=5):
    raise TimeoutError("MQTT publisher could not connect to broker")

i = 0
while True:
    temp = 28.3 + i * 0.5
    humidity = 20 + i * 5
    status = "Extreme Danger" if temp > 33 else "Danger" if temp > 30 else "Normal"
    alert_changed = 'true' if status != last_alert_status else 'false'
    last_alert_status = status
    payload = {
        "device": device_id,
        "temperature": round(temp, 2),
        "humidity": round(humidity, 2),
        "alert_status": status,
        "alert_changed": str(alert_changed)
    }

    result = client.publish(topic_telemetry, json.dumps(payload), qos=0)
    result.wait_for_publish()
    print("Sent a message")
    i += 1
    time.sleep(10)