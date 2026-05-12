let ws;
let selectedDevice = null;

let devices = {};
let streamData = [];
let rpcData = [];
let deviceState = {}; // ONLINE / OFFLINE

function formatTime(ts) {
    if (!ts) return "--";

    const value = Number(ts);
    if (!Number.isFinite(value)) return "--";

    const ms = value < 1e12 ? value * 1000 : value;
    const d = new Date(ms);
    return d.toLocaleTimeString();
}

function connectWS() {
    ws = new WebSocket(`ws://${location.host}/ws`);

    ws.onmessage = (event) => {
        const msg = JSON.parse(event.data);

        if (msg.type === "telemetry") handleTelemetry(msg);
        else if (msg.type === "rpc") handleRPC(msg);
        else if (msg.type === "device_status") {
            deviceState[msg.device] = {
                status: msg.status,
                ts: msg.ts
            };
            renderDevices();
        }
    };

    ws.onclose = () => setTimeout(connectWS, 2000);
}

function handleTelemetry(msg) {
    const { device, payload, ts } = msg;

    devices[device] = true;
    deviceState[device] = {
        status: deviceState[device]?.status || "ONLINE",
        ts: ts
    };
    streamData.unshift({ device, payload, ts });

    renderDevices();
    renderStream();
}

function handleRPC(msg) {
    rpcData.unshift(msg);
    renderRPC();
}

/* =========================
   DEVICE LIST
========================= */
function renderDevices() {

    const el = document.getElementById("deviceList");
    el.innerHTML = "";

    // merge devices seen from telemetry and explicit deviceState
    const keys = Array.from(new Set([...Object.keys(devices), ...Object.keys(deviceState)]));

    keys.forEach(d => {
        const st = deviceState[d] || { status: "ONLINE", ts: null };

        const signal = `
        <div class="signal ${st.status.toLowerCase()}">
            <span></span>
            <span></span>
            <span></span>
        </div>
        `;

        const div = document.createElement("div");
        div.className = "device" + (selectedDevice === d ? " active" : "");

        div.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <b>${d}</b>
                ${signal}
            </div>

            <div style="font-size:11px; color:#7a8b9a;">
                last: ${formatTime(st.ts)}
            </div>
        `;

        div.onclick = () => {
            selectedDevice = d;
            renderDevices();
            renderStream();
        };

        el.appendChild(div);
    });
}

/* =========================
   STREAM (🔥 đẹp hơn JSON)
========================= */
function renderStream() {
    const el = document.getElementById("stream");
    el.innerHTML = "";

    streamData
        .filter(x => !selectedDevice || x.device === selectedDevice)
        .slice(0, 3)
        .forEach(x => {

            const p = x.payload;
            const t = x.ts;

            const card = document.createElement("div");
            card.className = "card";

            card.innerHTML = `
                <div style="display:flex; justify-content:space-between;">
                    <span style="font-size:11px; color:#7a8b9a;">
                        ${formatTime(t)}
                    </span>
                </div>
                <div style="height:10px;"></div>
                <div class="kv">
                    <div>Temperature</div>
                    <div>${p.temperature} °C</div>

                    <div>Humidity</div>
                    <div>${p.humidity} %</div>

                    <div>Status</div>
                    <div>${p.alert_status}</div>
                </div>
            `;
            el.appendChild(card);
        });
}

/* =========================
   RPC
========================= */
function renderRPC() {
    const el = document.getElementById("rpc");
    el.innerHTML = "";

    rpcData.slice(0, 5).forEach(x => {

        const card = document.createElement("div");
        card.className = "card";

        card.innerHTML = `
            <div style="display:flex; justify-content:space-between;">
                <b>RPC → ${x.device}</b>
                <div style="height:10px;"></div>
                <span style="font-size:11px; color:#7a8b9a;">
                    ${formatTime(x.ts)}
                </span>
            </div>

            <div class="kv">
                <div>Payload</div>
                <div>${JSON.stringify(x.payload)}</div>
            </div>
        `;

        el.appendChild(card);
    });
}

connectWS();