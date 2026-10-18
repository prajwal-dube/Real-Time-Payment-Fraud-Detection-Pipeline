import express from 'express';
import http from 'node:http';
import { randomUUID } from 'node:crypto';
import { Kafka, logLevel } from 'kafkajs';
import { Server } from 'socket.io';
import { validate } from './validation.js';

const app = express();
app.disable('x-powered-by');
app.use(express.json({ limit: '32kb' }));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: process.env.DASHBOARD_ORIGIN || 'http://localhost:5173' } });
const kafka = new Kafka({ clientId: 'fraud-gateway', brokers: (process.env.KAFKA_BOOTSTRAP || 'localhost:9092').split(','), logLevel: logLevel.ERROR });
const producer = kafka.producer({ allowAutoTopicCreation: false });
const consumer = kafka.consumer({ groupId: 'fraud-dashboard-v1', allowAutoTopicCreation: false });
const recent = [];
const seen = new Set();
const order = [];
const counts = { APPROVED: 0, REVIEW: 0, BLOCKED: 0 };
const port = Number(process.env.PORT || 3000);

app.get('/health', (_req, res) => res.json({ ok: true }));
app.get('/api/recent', (_req, res) => res.json({ items: recent, counts }));
app.post('/api/transactions', async (req, res) => {
  const error = validate(req.body);
  if (error) return res.status(400).json({ error });
  const { cardId, amount, country, lat, lon, eventAt } = req.body;
  const event = { eventId: randomUUID(), cardId, amount, country, lat, lon, eventAt, ingestedAt: Date.now() };
  if (process.env.ALLOW_SYNTHETIC_TRUTH === 'true' && (req.body.groundTruth === 0 || req.body.groundTruth === 1))
    event.groundTruth = req.body.groundTruth;
  try {
    await producer.send({ topic: 'transactions', messages: [{ key: cardId, value: JSON.stringify(event) }] });
    return res.status(202).json({ eventId: event.eventId, status: 'QUEUED' });
  } catch (err) {
    console.error('Kafka publish failed:', err.message);
    return res.status(503).json({ error: 'Queue unavailable' });
  }
});

async function start() {
  await producer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: 'fraud-decisions', fromBeginning: true });
  await consumer.run({ eachMessage: async ({ message }) => {
    let decision;
    try { decision = JSON.parse(message.value.toString()); }
    catch { return; }
    if (!decision.eventId || !Object.hasOwn(counts, decision.decision)) return;
    if (seen.has(decision.eventId)) return;
    seen.add(decision.eventId); order.push(decision.eventId);
    if (order.length > 10000) seen.delete(order.shift());
    recent.unshift(decision);
    if (recent.length > 2000) recent.pop();
    counts[decision.decision]++;
    io.emit('fraud:decision', decision);
  } });
  server.listen(port, '0.0.0.0', () => console.log('API and Socket.IO on', port));
}
start().catch(error => { console.error(error); process.exit(1); });

async function shutdown() {
  server.close();
  await consumer.disconnect();
  await producer.disconnect();
  process.exit(0);
}
process.on('SIGTERM', shutdown);
