import type { Db } from "./db.ts";
import type { IncomingMessage, ServerResponse } from "node:http";

// ============================================================================
// TYPES
// ============================================================================

export interface AxonEvent {
  id: number;
  channel: string;
  source: string;
  type: string;
  payload: Record<string, unknown>;
  created_at: string;
}

export interface Subscription {
  id: number;
  agent: string;
  channel: string;
  filter_type: string | null;
  webhook_url: string | null;
  created_at: string;
}

interface SSEClient {
  agent: string;
  channels: Set<string>;
  filterType: string | null;
  res: ServerResponse;
  lastEventId: number;
}

// ============================================================================
// IN-MEMORY SSE CLIENTS
// ============================================================================

const sseClients: Map<string, SSEClient> = new Map();
let clientIdCounter = 0;

function broadcastToSSE(event: AxonEvent) {
  for (const [id, client] of sseClients) {
    if (!client.channels.has("*") && !client.channels.has(event.channel)) continue;
    if (client.filterType && event.type !== client.filterType) continue;
    try {
      client.res.write(`id: ${event.id}\nevent: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
    } catch {
      sseClients.delete(id);
    }
  }
}

// ============================================================================
// PUBLISH
// ============================================================================

export function publish(db: Db, channel: string, source: string, type: string, payload: Record<string, unknown>): AxonEvent {
  const row = db.prepare(
    "INSERT INTO events (channel, source, type, payload) VALUES (?, ?, ?, ?) RETURNING *"
  ).get(channel, source, type, JSON.stringify(payload)) as any;
  const event: AxonEvent = {
    id: row.id,
    channel: row.channel,
    source: row.source,
    type: row.type,
    payload: JSON.parse(row.payload),
    created_at: row.created_at,
  };

  // Fan out to SSE listeners
  broadcastToSSE(event);

  // Fan out to webhook subscribers
  fanOutWebhooks(db, event);

  return event;
}

// ============================================================================
// WEBHOOK FAN-OUT
// ============================================================================

function fanOutWebhooks(db: Db, event: AxonEvent) {
  const subs = db.prepare(
    "SELECT * FROM subscriptions WHERE channel = ? AND webhook_url IS NOT NULL"
  ).all(event.channel) as Subscription[];

  for (const sub of subs) {
    if (sub.filter_type && sub.filter_type !== event.type) continue;
    // Fire and forget
    fetch(sub.webhook_url!, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(event),
      signal: AbortSignal.timeout(5000),
    }).catch(() => {});
  }
}

// ============================================================================
// QUERY
// ============================================================================

export function getEvents(
  db: Db,
  opts: { channel?: string; source?: string; type?: string; since_id?: number; limit?: number }
): AxonEvent[] {
  let query = "SELECT * FROM events WHERE 1=1";
  const params: Array<string | number> = [];

  if (opts.channel) { query += " AND channel = ?"; params.push(opts.channel); }
  if (opts.source) { query += " AND source = ?"; params.push(opts.source); }
  if (opts.type) { query += " AND type = ?"; params.push(opts.type); }
  if (opts.since_id) { query += " AND id > ?"; params.push(opts.since_id); }

  query += " ORDER BY id DESC LIMIT ?";
  params.push(opts.limit ?? 50);

  const rows = db.prepare(query).all(...params) as any[];
  return rows.map(r => ({ ...r, payload: JSON.parse(r.payload) }));
}

export function getEvent(db: Db, id: number): AxonEvent | undefined {
  const row = db.prepare("SELECT * FROM events WHERE id = ?").get(id) as any;
  if (!row) return undefined;
  return { ...row, payload: JSON.parse(row.payload) };
}

// ============================================================================
// CHANNELS
// ============================================================================

export function listChannels(db: Db) {
  return db.prepare(`
    SELECT c.*, (SELECT COUNT(*) FROM events e WHERE e.channel = c.name) as event_count,
           (SELECT COUNT(*) FROM subscriptions s WHERE s.channel = c.name) as subscriber_count
    FROM channels c ORDER BY c.name
  `).all();
}

export function createChannel(db: Db, name: string, description?: string, retainHours?: number) {
  return db.prepare(
    "INSERT INTO channels (name, description, retain_hours) VALUES (?, ?, ?) RETURNING *"
  ).get(name, description ?? null, retainHours ?? 168);
}

// ============================================================================
// SUBSCRIPTIONS
// ============================================================================

export function subscribe(db: Db, agent: string, channel: string, filterType?: string, webhookUrl?: string): Subscription {
  return db.prepare(
    "INSERT INTO subscriptions (agent, channel, filter_type, webhook_url) VALUES (?, ?, ?, ?) ON CONFLICT(agent, channel) DO UPDATE SET filter_type = excluded.filter_type, webhook_url = excluded.webhook_url RETURNING *"
  ).get(agent, channel, filterType ?? null, webhookUrl ?? null) as Subscription;
}

export function unsubscribe(db: Db, agent: string, channel: string): boolean {
  return db.prepare("DELETE FROM subscriptions WHERE agent = ? AND channel = ?").run(agent, channel).changes > 0;
}

export function getSubscriptions(db: Db, agent?: string): Subscription[] {
  if (agent) {
    return db.prepare("SELECT * FROM subscriptions WHERE agent = ? ORDER BY channel").all(agent) as Subscription[];
  }
  return db.prepare("SELECT * FROM subscriptions ORDER BY channel, agent").all() as Subscription[];
}

// ============================================================================
// CURSORS (poll-based consumption)
// ============================================================================

export function poll(db: Db, agent: string, channel: string, limit: number = 50): { events: AxonEvent[]; cursor: number } {
  const cursor = db.prepare("SELECT last_event_id FROM cursors WHERE agent = ? AND channel = ?").get(agent, channel) as any;
  const sinceId = cursor?.last_event_id ?? 0;

  const rows = db.prepare(
    "SELECT * FROM events WHERE channel = ? AND id > ? ORDER BY id ASC LIMIT ?"
  ).all(channel, sinceId, limit) as any[];

  const events = rows.map(r => ({ ...r, payload: JSON.parse(r.payload) }));

  if (events.length > 0) {
    const newCursor = events[events.length - 1].id;
    db.prepare(
      "INSERT INTO cursors (agent, channel, last_event_id, updated_at) VALUES (?, ?, ?, datetime('now')) ON CONFLICT(agent, channel) DO UPDATE SET last_event_id = excluded.last_event_id, updated_at = datetime('now')"
    ).run(agent, channel, newCursor);
  }

  return { events, cursor: events.length > 0 ? events[events.length - 1].id : sinceId };
}

// ============================================================================
// SSE STREAM
// ============================================================================

export function startSSE(
  req: IncomingMessage,
  res: ServerResponse,
  agent: string,
  channels: string[],
  filterType?: string,
  lastEventId?: number,
) {
  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache",
    "Connection": "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.write(":ok\n\n");

  const clientId = `sse-${++clientIdCounter}`;
  const client: SSEClient = {
    agent,
    channels: new Set(channels),
    filterType: filterType ?? null,
    res,
    lastEventId: lastEventId ?? 0,
  };
  sseClients.set(clientId, client);

  req.on("close", () => { sseClients.delete(clientId); });

  // Send heartbeat every 30s
  const heartbeat = setInterval(() => {
    try { res.write(":heartbeat\n\n"); } catch { clearInterval(heartbeat); sseClients.delete(clientId); }
  }, 30000);
  req.on("close", () => clearInterval(heartbeat));
}

// ============================================================================
// MAINTENANCE
// ============================================================================

export function pruneEvents(db: Db) {
  const channels = db.prepare("SELECT name, retain_hours FROM channels").all() as Array<{ name: string; retain_hours: number }>;
  let total = 0;
  for (const ch of channels) {
    const result = db.prepare(
      "DELETE FROM events WHERE channel = ? AND created_at < datetime('now', ?)"
    ).run(ch.name, `-${ch.retain_hours} hours`);
    total += result.changes;
  }
  return total;
}

export function getStats(db: Db) {
  const eventCount = (db.prepare("SELECT COUNT(*) as c FROM events").get() as any).c;
  const channelCount = (db.prepare("SELECT COUNT(*) as c FROM channels").get() as any).c;
  const subCount = (db.prepare("SELECT COUNT(*) as c FROM subscriptions").get() as any).c;
  const activeSSE = sseClients.size;
  return { events: eventCount, channels: channelCount, subscriptions: subCount, sse_clients: activeSSE };
}
