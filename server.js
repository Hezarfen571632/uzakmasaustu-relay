const http = require("http");
const WebSocket = require("ws");

const PORT = process.env.PORT || 8080;

// ═══════════════════════════════════════════════════════
// State
// ═══════════════════════════════════════════════════════
const clients = new Map();  // id -> { ws, hostname }
const pairs = new Map();    // id -> partnerId

// ═══════════════════════════════════════════════════════
// HTTP Server (health check for Render.com)
// ═══════════════════════════════════════════════════════
const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "application/json" });
  res.end(JSON.stringify({
    status: "ok",
    clients: clients.size,
    pairs: pairs.size / 2,
    uptime: process.uptime(),
  }));
});

// ═══════════════════════════════════════════════════════
// WebSocket Relay
// ═══════════════════════════════════════════════════════
const wss = new WebSocket.Server({
  server,
  maxPayload: 10 * 1024 * 1024, // 10MB max message size for screen frames
});

wss.on("connection", (ws) => {
  let clientId = null;
  let forwardCount = 0;

  ws.on("message", (raw, isBinary) => {
    // Binary messages are encrypted screen frames — forward directly
    if (isBinary) {
      const partnerId = pairs.get(clientId);
      if (partnerId) {
        const partner = clients.get(partnerId);
        if (partner && partner.ws.readyState === WebSocket.OPEN) {
          partner.ws.send(raw, { binary: true });
          forwardCount++;
          if (forwardCount % 30 === 1) {
            console.log(`[→BIN] ${clientId} -> ${partnerId}: binary frame #${forwardCount} (${Math.round(raw.length/1024)}KB)`);
          }
        }
      }
      return;
    }

    // Text messages are JSON commands
    const strData = raw.toString("utf8");
    let msg;
    try { msg = JSON.parse(strData); } catch { return; }

    switch (msg.type) {
      // Client registers with its connection ID
      case "Register": {
        clientId = msg.id;
        // If an old connection exists for this ID, close it cleanly
        const existing = clients.get(clientId);
        if (existing && existing.ws !== ws && existing.ws.readyState === WebSocket.OPEN) {
          existing.ws._replaced = true;
          existing.ws.close();
        }
        clients.set(clientId, { ws, hostname: msg.hostname || "Unknown" });
        ws.send(JSON.stringify({ type: "Registered", id: clientId }));
        console.log(`[+] ${clientId} registered (${clients.size} online)`);
        break;
      }

      // Client wants to connect to a peer by ID
      case "ConnectTo": {
        const target = clients.get(msg.target_id);
        if (target && target.ws.readyState === WebSocket.OPEN) {
          // Pair them
          pairs.set(clientId, msg.target_id);
          pairs.set(msg.target_id, clientId);

          // Notify target (host)
          target.ws.send(JSON.stringify({
            type: "IncomingPeer",
            peer_id: clientId,
            peer_hostname: clients.get(clientId)?.hostname || "Unknown",
          }));

          // Confirm to requester (client)
          ws.send(JSON.stringify({
            type: "Paired",
            peer_id: msg.target_id,
            peer_hostname: target.hostname,
          }));

          console.log(`[⇄] Paired: ${clientId} <-> ${msg.target_id}`);
        } else {
          ws.send(JSON.stringify({
            type: "PeerNotFound",
            target_id: msg.target_id,
          }));
          console.log(`[!] PeerNotFound: ${msg.target_id} (requested by ${clientId})`);
        }
        break;
      }

      // Any other message (Encrypted frames, etc.) → forward to paired partner
      default: {
        const partnerId = pairs.get(clientId);
        if (partnerId) {
          const partner = clients.get(partnerId);
          if (partner && partner.ws.readyState === WebSocket.OPEN) {
            // ALWAYS send as string (text frame) so Rust receives Message::Text
            partner.ws.send(strData, { binary: false });
            forwardCount++;
            if (forwardCount % 30 === 1) {
              console.log(`[→] ${clientId} -> ${partnerId}: frame #${forwardCount} (${Math.round(strData.length/1024)}KB)`);
            }
          } else {
            console.log(`[!] Partner ${partnerId} not available for ${clientId}`);
          }
        } else {
          // Not paired yet, ignore
          if (msg.type !== "Heartbeat") {
            console.log(`[?] No pair for ${clientId}, dropping ${msg.type || "unknown"} message`);
          }
        }
        break;
      }
    }
  });

  ws.on("close", () => {
    if (clientId) {
      // Skip cleanup if this WS was replaced by a newer connection
      if (ws._replaced) return;
      // Only cleanup if this WS is still the registered one
      const current = clients.get(clientId);
      if (current && current.ws !== ws) return;

      // Notify partner
      const partnerId = pairs.get(clientId);
      if (partnerId) {
        const partner = clients.get(partnerId);
        if (partner && partner.ws.readyState === WebSocket.OPEN) {
          partner.ws.send(JSON.stringify({ type: "PeerDisconnected" }));
        }
        pairs.delete(partnerId);
      }
      pairs.delete(clientId);
      clients.delete(clientId);
      console.log(`[-] ${clientId} disconnected (${clients.size} online, forwarded ${forwardCount} msgs)`);
    }
  });

  ws.on("error", (err) => {
    console.log(`[E] WS error for ${clientId}: ${err.message}`);
  });
});

// Keep-alive: ping all clients every 30 seconds
setInterval(() => {
  wss.clients.forEach((ws) => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.ping();
    }
  });
}, 30000);

server.listen(PORT, () => {
  console.log(`UzakMasaüstü Relay Server running on port ${PORT}`);
});
