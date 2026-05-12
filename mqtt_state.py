mqtt_messages = []
connected_devices = set()
system_stats = {
    "total_messages": 0,
    "start_time": 0
}

# device status tracking
device_last_seen = {}
device_status = {}  # ONLINE / OFFLINE
OFFLINE_THRESHOLD = 30  # seconds