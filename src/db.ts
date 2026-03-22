import Database from "libsql";

export function initDb(path: string): InstanceType<typeof Database> {
  const db = new Database(path);
  db.pragma("journal_mode = WAL");
  db.pragma("busy_timeout = 5000");
  db.pragma("synchronous = NORMAL");

  db.exec(`
    -- Channels are named topics agents publish/subscribe to
    CREATE TABLE IF NOT EXISTS channels (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      name TEXT NOT NULL UNIQUE,
      description TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      retain_hours INTEGER NOT NULL DEFAULT 168
    );

    -- Events are messages published to channels
    CREATE TABLE IF NOT EXISTS events (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      channel TEXT NOT NULL,
      source TEXT NOT NULL,
      type TEXT NOT NULL,
      payload TEXT NOT NULL DEFAULT '{}',
      created_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE INDEX IF NOT EXISTS idx_events_channel ON events(channel, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_events_source ON events(source, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_events_type ON events(type, created_at DESC);
    CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);

    -- Subscriptions track which agents listen to which channels
    CREATE TABLE IF NOT EXISTS subscriptions (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      agent TEXT NOT NULL,
      channel TEXT NOT NULL,
      filter_type TEXT,
      webhook_url TEXT,
      created_at TEXT NOT NULL DEFAULT (datetime('now')),
      UNIQUE(agent, channel)
    );
    CREATE INDEX IF NOT EXISTS idx_subs_agent ON subscriptions(agent);
    CREATE INDEX IF NOT EXISTS idx_subs_channel ON subscriptions(channel);

    -- Cursors track where each agent has read up to per channel
    CREATE TABLE IF NOT EXISTS cursors (
      agent TEXT NOT NULL,
      channel TEXT NOT NULL,
      last_event_id INTEGER NOT NULL DEFAULT 0,
      updated_at TEXT NOT NULL DEFAULT (datetime('now')),
      PRIMARY KEY (agent, channel)
    );

    -- Seed default channels
    INSERT OR IGNORE INTO channels (name, description) VALUES
      ('system', 'System-wide events: agent online/offline, errors, health'),
      ('memory', 'Engram memory events: store, search, link, forget'),
      ('tasks', 'Chiasm task events: created, updated, completed, blocked'),
      ('deploy', 'Deployment events: started, succeeded, failed, rolled back'),
      ('alerts', 'Alert events: threshold breaches, anomalies, incidents');
  `);

  return db;
}

export type Db = InstanceType<typeof Database>;
