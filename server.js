/**
 * server.js
 *
 * Privacy-focused, zero-retention WebSocket messaging relay.
 *
 * Design goals:
 *  - No persistence layer, no disk writes, no database. Everything lives in
 *    process memory and disappears on restart.
 *  - The server is a "blind pipe": it looks at the `to` field to route a
 *    packet, but never inspects, logs, or stores `payload` contents beyond
 *    the transient in-memory offline queue described below.
 *  - Offline delivery is best-effort and temporary: messages for an offline
 *    handle sit in memory only until that handle reconnects, at which point
 *    they are flushed and immediately discarded.
 *
 * Protocol (JSON text frames):
 *   Register:   { "type": "register", "handle": "@myname" }
 *   Send:       { "to": "@friend", "payload": "..." }         (any JSON-able payload)
 *   Delivered:  { "type": "message", "from": "@sender", "payload": "..." }
 *   Queued ack: { "type": "queued", "to": "@friend" }
 *   Error:      { "type": "error", "message": "..." }
 *   Info:       { "type": "info", "message": "..." }
 */

'use strict';

const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 8080;

// handle -> WebSocket connection (only online, registered clients)
const clients = new Map();

// handle -> array of queued packets waiting for that handle to come online
// Each queued packet: { from, payload, queuedAt }
const offlineQueues = new Map();

// Basic safety limits to keep memory bounded on a public-facing relay.
const MAX_HANDLE_LENGTH = 64;
const MAX_QUEUE_PER_HANDLE = 100;
const MAX_PAYLOAD_BYTES = 64 * 1024; // 64 KB per message

const wss = new WebSocketServer({ port: PORT });

console.log(`[relay] zero-retention message relay listening on port ${PORT}`);

wss.on('connection', (ws) => {
  // Each socket gets a mutable slot for its handle once registered.
  // Kept on the socket object itself (not in any external store) so it
  // vanishes along with the socket when the connection closes.
  ws.handle = null;

  ws.on('message', (raw) => {
    // Reject oversized frames early to avoid buffering abuse.
    if (raw.length > MAX_PAYLOAD_BYTES) {
      sendError(ws, 'payload too large');
      return;
    }

    let packet;
    try {
      packet = JSON.parse(raw.toString());
    } catch (err) {
      sendError(ws, 'invalid JSON');
      return;
    }

    if (!packet || typeof packet !== 'object') {
      sendError(ws, 'invalid packet');
      return;
    }

    if (packet.type === 'register') {
      handleRegister(ws, packet);
      return;
    }

    if (typeof packet.to === 'string') {
      handleRoute(ws, packet);
      return;
    }

    sendError(ws, 'unrecognized packet shape');
  });

  ws.on('close', () => {
    handleDisconnect(ws);
  });

  ws.on('error', () => {
    // Treat socket errors the same as a disconnect: clean up and move on.
    handleDisconnect(ws);
  });
});

/**
 * Registers a client under a handle, taking over any existing connection
 * for that handle, then flushes and clears any messages that queued up
 * while the handle was offline.
 */
function handleRegister(ws, packet) {
  const handle = packet.handle;

  if (typeof handle !== 'string' || handle.length === 0 || handle.length > MAX_HANDLE_LENGTH) {
    sendError(ws, 'invalid handle');
    return;
  }

  // If this handle already has a live connection (e.g. reconnect from a new
  // tab), close the stale one so the map never leaks duplicate sockets.
  const existing = clients.get(handle);
  if (existing && existing !== ws) {
    try {
      existing.close(4000, 'replaced by new connection');
    } catch (_) {
      // ignore - we're discarding this socket anyway
    }
  }

  ws.handle = handle;
  clients.set(handle, ws);

  sendInfo(ws, `registered as ${handle}`);

  // Flush any messages that arrived while this handle was offline, then
  // clear the queue immediately - nothing is retained after delivery.
  const queue = offlineQueues.get(handle);
  if (queue && queue.length > 0) {
    for (const item of queue) {
      send(ws, {
        type: 'message',
        from: item.from,
        payload: item.payload,
      });
    }
    offlineQueues.delete(handle);
  }
}

/**
 * Routes a packet from a registered sender to its target handle. If the
 * target is online, the packet is forwarded immediately without being
 * written anywhere. If offline, it is placed in a bounded in-memory queue
 * for later delivery.
 */
function handleRoute(ws, packet) {
  if (!ws.handle) {
    sendError(ws, 'must register before sending messages');
    return;
  }

  const to = packet.to;
  const payload = packet.payload;

  if (typeof to !== 'string' || to.length === 0) {
    sendError(ws, 'invalid "to" handle');
    return;
  }

  const outgoing = {
    type: 'message',
    from: ws.handle,
    payload,
  };

  const target = clients.get(to);

  if (target && target.readyState === target.OPEN) {
    // Blind pipe: forward immediately, no storage, no inspection of payload.
    send(target, outgoing);
    return;
  }

  // Target offline: queue temporarily until they reconnect.
  let queue = offlineQueues.get(to);
  if (!queue) {
    queue = [];
    offlineQueues.set(to, queue);
  }

  if (queue.length >= MAX_QUEUE_PER_HANDLE) {
    // Drop the oldest message to bound memory rather than growing forever.
    queue.shift();
  }

  queue.push({ from: ws.handle, payload });

  sendInfo(ws, `${to} is offline; message queued`, { type: 'queued', to });
}

/**
 * Removes a disconnected client from the active map. Offline queues for
 * that handle are intentionally left in place (they still represent
 * messages waiting to be delivered on the next reconnect) but everything
 * about the live connection itself is discarded.
 */
function handleDisconnect(ws) {
  if (ws.handle && clients.get(ws.handle) === ws) {
    clients.delete(ws.handle);
  }
}

// --- small send helpers -----------------------------------------------

function send(ws, obj) {
  if (ws.readyState === ws.OPEN) {
    ws.send(JSON.stringify(obj));
  }
}

function sendError(ws, message) {
  send(ws, { type: 'error', message });
}

function sendInfo(ws, message, extra) {
  send(ws, Object.assign({ type: 'info', message }, extra));
}

// --- graceful shutdown ---------------------------------------------------

function shutdown() {
  console.log('[relay] shutting down, closing all connections');
  for (const ws of clients.values()) {
    try {
      ws.close(1001, 'server shutting down');
    } catch (_) {
      // ignore
    }
  }
  wss.close(() => process.exit(0));
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
  
