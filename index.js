import "dotenv/config";
import makeWASocket, {
  Browsers,
  DisconnectReason,
  useMultiFileAuthState,
} from "@whiskeysockets/baileys";
import QRCode from "qrcode-terminal";
import pino from "pino";

const {
  AGIE_INGEST_URL,
  AGIE_INGEST_SECRET,
  AGIE_WORKER_ID = "agie-gateway-replit",
  AGIE_PHONE_NUMBER = "",
  AGIE_PAIRING_PHONE = "",
} = process.env;

if (!AGIE_INGEST_URL || !AGIE_INGEST_SECRET) {
  console.error("Missing AGIE_INGEST_URL or AGIE_INGEST_SECRET in Replit Secrets.");
  process.exit(1);
}

const log = pino({ level: process.env.LOG_LEVEL || "info" });
const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS || 30_000);
const FLUSH_MS = Number(process.env.FLUSH_MS || 60_000);

let lastLatencyMs = 0;
let eventsSinceBeat = 0;

async function ingest(events) {
  if (!events.length) return;
  const started = Date.now();
  try {
    const res = await fetch(AGIE_INGEST_URL, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-agie-secret": AGIE_INGEST_SECRET,
      },
      body: JSON.stringify({ events }),
    });
    lastLatencyMs = Date.now() - started;
    eventsSinceBeat += events.length;
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      log.warn({ status: res.status, body }, "Ingest rejected");
    } else {
      log.info({ applied: body.applied }, "Events ingested successfully");
    }
  } catch (err) {
    log.error({ err: String(err) }, "Ingest request failed");
  }
}

const counters = new Map();
const knownGroups = new Map();
const pendingEvents = [];

const dayKeyOf = (d = new Date()) => d.toISOString().slice(0, 10);
const weekKeyOf = (d = new Date()) => {
  const date = new Date(Date.UTC(d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate()));
  date.setUTCDate(date.getUTCDate() - ((date.getUTCDay() + 6) % 7));
  return date.toISOString().slice(0, 10);
};
const monthKeyOf = (d = new Date()) => d.toISOString().slice(0, 7);

function bucketFor(groupJid, memberJid) {
  if (!counters.has(groupJid)) counters.set(groupJid, new Map());
  const members = counters.get(groupJid);
  if (!members.has(memberJid)) {
    members.set(memberJid, {
      name: "", day: 0, week: 0, month: 0,
      dayKey: dayKeyOf(), weekKey: weekKeyOf(), monthKey: monthKeyOf(),
      lastActive: null,
    });
  }
  return members.get(memberJid);
}

function recordMessage(groupJid, memberJid, name) {
  const now = new Date();
  const b = bucketFor(groupJid, memberJid);
  if (b.dayKey !== dayKeyOf(now)) { b.day = 0; b.dayKey = dayKeyOf(now); }
  if (b.weekKey !== weekKeyOf(now)) { b.week = 0; b.weekKey = weekKeyOf(now); }
  if (b.monthKey !== monthKeyOf(now)) { b.month = 0; b.monthKey = monthKeyOf(now); }
  b.day++; b.week++; b.month++;
  if (name) b.name = name;
  b.lastActive = now.toISOString();
}

function queue(event) {
  pendingEvents.push(event);
  if (pendingEvents.length >= 50) void flush();
}

function phoneDigits(jid) {
  return (jid || "").split(":")[0].replace(/\D+/g, "");
}

function flushMembers() {
  const now = new Date();
  for (const [groupJid, members] of counters) {
    const list = [...members.entries()].map(([jid, b]) => ({
      jid,
      displayName: b.name || jid.split("@")[0],
      messagesDay: b.day,
      messagesWeek: b.week,
      messagesMonth: b.month,
      lastActiveAt: b.lastActive || undefined,
    }));
    queue({
      kind: "member.stats",
      phoneNumber: currentPhone || AGIE_PHONE_NUMBER,
      groupJid,
      members: list,
    });
  }
}

function heartbeat() {
  queue({
    kind: "worker.heartbeat",
    workerId: AGIE_WORKER_ID,
    name: AGIE_WORKER_ID,
    status: "operational",
    eventsPerSec: Math.round(eventsSinceBeat / (HEARTBEAT_MS / 1000)),
    queueDepth: pendingEvents.length,
    latencyMs: lastLatencyMs,
  });
  eventsSinceBeat = 0;
}

async function flush() {
  heartbeat();
  flushMembers();
  const batch = pendingEvents.splice(0, pendingEvents.length);
  await ingest(batch);
}

setInterval(() => void flush(), FLUSH_MS);

let currentPhone = "";

async function start() {
  const { state, saveCreds } = await useMultiFileAuthState("./agie-session");
  const sock = makeWASocket({
    auth: state,
    browser: Browsers.ubuntu("Chrome"),
    printQRInTerminal: false,
    logger: pino({ level: "silent" }),
  });

  sock.ev.on("creds.update", saveCreds);

  sock.ev.on("connection.update", async (update) => {
    const { connection, lastDisconnect, qr } = update;

    if (qr) {
      QRCode.generate(qr, { small: true });
      console.log("\nScan this QR with WhatsApp (Settings → Linked devices → Link a device)\n");
      if (AGIE_PAIRING_PHONE) {
        const code = await sock.requestPairingCode(AGIE_PAIRING_PHONE.replace(/\D+/g, ""));
        console.log(`Pairing code: ${code}\n`);
      }
      queue({
        kind: "session.status",
        phoneNumber: currentPhone || AGIE_PHONE_NUMBER,
        status: "qr_pending",
        pairingState: "qr",
      });
      void flush();
    }

    if (connection === "open") {
      currentPhone = phoneDigits(sock.user?.id) || AGIE_PHONE_NUMBER;
      log.info({ phone: currentPhone }, "WhatsApp connected");
      queue({
        kind: "session.status",
        phoneNumber: currentPhone,
        status: "connected",
        lastSeenAt: new Date().toISOString(),
      });

      try {
        const all = await sock.groupFetchAllParticipating();
        for (const [jid, meta] of Object.entries(all)) {
          knownGroups.set(jid, { name: meta.subject || jid, size: meta.participants?.length ?? 0 });
          queue({
            kind: "group.snapshot",
            phoneNumber: currentPhone,
            groupJid: jid,
            name: meta.subject || jid,
            memberCount: meta.participants?.length ?? 0,
          });
        }
      } catch (err) {
        log.error({ err: String(err) }, "Group fetch failed");
      }
      void flush();
    }

    if (connection === "close") {
      const code = lastDisconnect?.error?.output?.statusCode;
      const rejoin = code !== DisconnectReason.loggedOut;
      queue({
        kind: "session.status",
        phoneNumber: currentPhone || AGIE_PHONE_NUMBER,
        status: "disconnected",
        pairingState: rejoin ? "reconnecting" : "none",
        lastSeenAt: new Date().toISOString(),
      });
      void flush();
      if (rejoin) setTimeout(() => void start(), 3_000);
    }
  });

  sock.ev.on("messages.upsert", async ({ messages, type }) => {
    if (type !== "notify") return;
    for (const msg of messages) {
      const jid = msg.key.remoteJid;
      if (!jid?.endsWith("@g.us") || msg.key.fromMe) continue;
      const sender = msg.key.participant || jid;
      recordMessage(jid, sender, msg.pushName || "");
    }
  });

  sock.ev.on("groups.upsert", (groups) => {
    if (!currentPhone) return;
    for (const g of groups) {
      knownGroups.set(g.id, { name: g.subject || g.id, size: g.participants?.length ?? 0 });
      queue({
        kind: "group.snapshot",
        phoneNumber: currentPhone,
        groupJid: g.id,
        name: g.subject || g.id,
        memberCount: g.participants?.length ?? 0,
      });
    }
  });
}

void start();
