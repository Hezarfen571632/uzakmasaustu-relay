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
const wss = new WebSocket.Server({ server });

wss.on("connection", (ws) => {
  let clientId = null;

  ws.on("message", (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    switch (msg.type) {
      // Client registers with its connection ID
      case "Register": {
        clientId = msg.id;
        // If an old connection exists for this ID, close it cleanly
        const existing = clients.get(clientId);
        if (existing && existing.ws !== ws && existing.ws.readyState === WebSocket.OPEN) {
          existing.ws._replaced = true; // Mark as replaced so close handler skips cleanup
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

          // Notify target
          target.ws.send(JSON.stringify({
            type: "IncomingPeer",
            peer_id: clientId,
            peer_hostname: clients.get(clientId)?.hostname || "Unknown",
          }));

          // Confirm to requester
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
        }
        break;
      }

      // Any other message → forward to paired partner as TEXT
      default: {
        const partnerId = pairs.get(clientId);
        if (partnerId) {
          const partner = clients.get(partnerId);
          if (partner && partner.ws.readyState === WebSocket.OPEN) {
            // CRITICAL: Convert Buffer to string so it's sent as text frame
            // Rust client only handles Message::Text, not Message::Binary
            partner.ws.send(raw.toString());
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
      console.log(`[-] ${clientId} disconnected (${clients.size} online)`);
    }
  });

  ws.on("error", () => {});
});

server.listen(PORT, () => {
  console.log(`UzakMasaüstü Relay Server running on port ${PORT}`);
});
