'use strict';
// billing: hive-compose e2e fixture "project B". Consumes orders from Kafka
// (topic ${TOPIC_PREFIX}orders), turns each into an invoice kept in memory and
// serves them on GET /invoices. Tracing is set up by tracing.js, which must be
// loaded first: `node --require ./tracing.js index.js`.
//
// Env: PORT (8080), KAFKA_BOOTSTRAP (localhost:9092, comma list), TOPIC_PREFIX,
//      KAFKA_GROUP_ID (${TOPIC_PREFIX}billing), UNIT_PRICE (10), HIVE_RUN_ID,
//      OTEL_* (see tracing.js).

const http = require('node:http');
const { Kafka, logLevel } = require('kafkajs');
const {
  context,
  propagation,
  trace,
  SpanKind,
  SpanStatusCode,
} = require('@opentelemetry/api');
const tracing = require('./tracing.js'); // cached when preloaded via --require

function env(name, dflt) {
  const v = process.env[name];
  return v === undefined || v.trim() === '' ? dflt : v.trim();
}

const PORT = Number(env('PORT', '8080'));
const BOOTSTRAP = env('KAFKA_BOOTSTRAP', 'localhost:9092')
  .split(',')
  .map((s) => s.trim())
  .filter(Boolean);
const TOPIC_PREFIX = env('TOPIC_PREFIX', '');
const TOPIC = `${TOPIC_PREFIX}orders`;
const GROUP_ID = env('KAFKA_GROUP_ID', `${TOPIC_PREFIX}billing`);
const UNIT_PRICE = Number(env('UNIT_PRICE', '10'));

const tracer = trace.getTracer('billing');

// orderId -> invoice; keyed so a redelivered message does not double-bill.
const invoices = new Map();
const kafkaState = { status: 'starting', error: null, consumed: 0 };
let shuttingDown = false;

// kafkajs header values are Buffers (or arrays of them).
const headerGetter = {
  keys: (carrier) => Object.keys(carrier || {}),
  get: (carrier, key) => {
    if (!carrier) return undefined;
    const k = Object.keys(carrier).find((h) => h.toLowerCase() === key.toLowerCase());
    if (k === undefined) return undefined;
    const v = carrier[k];
    const one = Array.isArray(v) ? v[0] : v;
    return one === undefined || one === null ? undefined : one.toString();
  },
};

function handleMessage({ topic, partition, message }) {
  const parentCtx = propagation.extract(context.active(), message.headers, headerGetter);
  const bag = propagation.getBaggage(parentCtx);
  const testId = bag && bag.getEntry('test.id') ? bag.getEntry('test.id').value : null;

  return tracer.startActiveSpan(
    'billing process',
    {
      kind: SpanKind.CONSUMER,
      attributes: {
        'messaging.system': 'kafka',
        'messaging.destination.name': topic,
        'messaging.operation.type': 'process',
        'messaging.consumer.group.name': GROUP_ID,
        'messaging.destination.partition.id': String(partition),
        'messaging.kafka.offset': Number(message.offset),
        ...(testId ? { 'test.id': testId, 'hive.test.id': testId } : {}),
      },
    },
    parentCtx,
    (span) => {
      try {
        const order = JSON.parse(message.value ? message.value.toString() : '');
        const qty = Number(order.qty) || 0;
        const invoice = {
          orderId: String(order.id),
          sku: String(order.sku),
          qty,
          amount: qty * UNIT_PRICE,
          testId,
        };
        span.setAttributes({
          'order.id': invoice.orderId,
          'order.sku': invoice.sku,
          'order.qty': invoice.qty,
          'invoice.amount': invoice.amount,
        });
        invoices.set(invoice.orderId, invoice);
        kafkaState.consumed += 1;
      } catch (err) {
        // A poison message is recorded and skipped, never retried forever.
        span.recordError(err);
        span.setStatus({ code: SpanStatusCode.ERROR, message: 'invalid order message' });
        console.error(`[billing] skipping invalid message at ${topic}/${partition}@${message.offset}: ${err.message}`);
      } finally {
        span.end();
      }
    },
  );
}

const kafka = new Kafka({
  clientId: 'billing',
  brokers: BOOTSTRAP,
  logLevel: logLevel.WARN,
  connectionTimeout: 3000,
  retry: { initialRetryTime: 300, maxRetryTime: 5000, retries: 8 },
});
const consumer = kafka.consumer({
  groupId: GROUP_ID,
  allowAutoTopicCreation: true,
  retry: { initialRetryTime: 300, maxRetryTime: 5000, retries: 8 },
});

consumer.on(consumer.events.GROUP_JOIN, () => {
  kafkaState.status = 'consuming';
  kafkaState.error = null;
});
consumer.on(consumer.events.CRASH, ({ payload }) => {
  kafkaState.status = 'crashed';
  kafkaState.error = String(payload && payload.error ? payload.error.message : payload);
  // kafkajs restarts by itself on retriable errors; otherwise reconnect here.
  if (!shuttingDown && payload && payload.restart === false) {
    setTimeout(() => startConsumer(), 2000).unref();
  }
});

let attempt = 0;
async function startConsumer() {
  while (!shuttingDown) {
    attempt += 1;
    try {
      kafkaState.status = 'connecting';
      await consumer.connect();
      await consumer.subscribe({ topics: [TOPIC], fromBeginning: true });
      await consumer.run({ eachMessage: handleMessage });
      kafkaState.status = 'running';
      console.log(`[billing] consumer running (brokers=${BOOTSTRAP.join(',')} topic=${TOPIC} group=${GROUP_ID})`);
      return;
    } catch (err) {
      kafkaState.status = 'retrying';
      kafkaState.error = err.message;
      const delay = Math.min(10000, 500 * 2 ** Math.min(attempt, 5));
      console.warn(`[billing] kafka not ready (attempt ${attempt}): ${err.message}; retrying in ${delay}ms`);
      try {
        await consumer.disconnect();
      } catch (_) {
        /* ignore */
      }
      await new Promise((r) => setTimeout(r, delay));
    }
  }
}

function sendJSON(res, status, body) {
  const data = JSON.stringify(body);
  res.writeHead(status, { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(data) });
  res.end(data);
}

const server = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');
  if (req.method === 'GET' && url.pathname === '/healthz') {
    // Always 200: the HTTP side is up; Kafka readiness is reported, not gated.
    return sendJSON(res, 200, { status: 'ok', kafka: kafkaState.status, kafkaError: kafkaState.error, consumed: kafkaState.consumed });
  }
  if (req.method === 'GET' && url.pathname === '/invoices') {
    const testId = url.searchParams.get('testId');
    let list = Array.from(invoices.values());
    if (testId) list = list.filter((i) => i.testId === testId);
    return sendJSON(res, 200, list);
  }
  return sendJSON(res, 404, { error: 'not found' });
});

server.listen(PORT, () => {
  console.log(`[billing] listening on :${PORT}`);
  startConsumer();
});

async function shutdown(signal) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`[billing] ${signal}: shutting down`);
  const hardExit = setTimeout(() => process.exit(1), 10000);
  hardExit.unref();
  await new Promise((r) => server.close(() => r()));
  try {
    // Bounded: a disconnect racing an in-flight (retrying) connect can stall.
    await Promise.race([
      consumer.disconnect(),
      new Promise((_, rej) => setTimeout(() => rej(new Error('disconnect timed out')), 4000).unref()),
    ]);
  } catch (err) {
    console.error('[billing] consumer disconnect failed:', err.message);
  }
  await tracing.shutdown();
  process.exit(0);
}

process.on('SIGTERM', () => shutdown('SIGTERM'));
process.on('SIGINT', () => shutdown('SIGINT'));
