'use strict';
// OpenTelemetry bootstrap for the billing fixture ("project B").
// Loaded before the app via `node --require ./tracing.js index.js`.
//
// Configured ONLY from the standard OTEL_* environment variables:
//   OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES   -> resource
//   OTEL_EXPORTER_OTLP_ENDPOINT / _TRACES_ENDPOINT -> OTLP/HTTP exporter
//   OTEL_TRACES_EXPORTER (otlp | console | none, comma list; default otlp)
//   OTEL_PROPAGATORS (default: W3C tracecontext + baggage)
// No endpoint is hardcoded. Like the orders fixture, when the exporter is
// otlp and no OTLP endpoint env var is set, spans are dropped instead of being
// sent to a guessed default.

const { NodeSDK } = require('@opentelemetry/sdk-node');
const { getNodeAutoInstrumentations } = require('@opentelemetry/auto-instrumentations-node');
const { OTLPTraceExporter } = require('@opentelemetry/exporter-trace-otlp-http');
const { BatchSpanProcessor, ConsoleSpanExporter } = require('@opentelemetry/sdk-trace');
const {
  CompositePropagator,
  W3CTraceContextPropagator,
  W3CBaggagePropagator,
} = require('@opentelemetry/core');
const { propagation } = require('@opentelemetry/api');

function env(name) {
  const v = process.env[name];
  return v === undefined || v.trim() === '' ? undefined : v.trim();
}

// Copies baggage member test.id onto every span at start, as test.id and
// hive.test.id (the hive-compose otel capability's name), plus env HIVE_RUN_ID.
class TestIdSpanProcessor {
  constructor(runId) {
    this.runId = runId;
  }
  onStart(span, parentContext) {
    const bag = parentContext && propagation.getBaggage(parentContext);
    const entry = bag && bag.getEntry('test.id');
    if (entry && entry.value) {
      span.setAttribute('test.id', entry.value);
      span.setAttribute('hive.test.id', entry.value);
    }
    if (this.runId) span.setAttribute('hive.run.id', this.runId);
  }
  onEnd() {}
  forceFlush() {
    return Promise.resolve();
  }
  shutdown() {
    return Promise.resolve();
  }
}

function exporterProcessors() {
  const names = (env('OTEL_TRACES_EXPORTER') || 'otlp')
    .split(',')
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean);
  const out = [];
  for (const name of names) {
    if (name === 'none') return [];
    if (name === 'otlp') {
      if (env('OTEL_EXPORTER_OTLP_ENDPOINT') || env('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT')) {
        // OTLPTraceExporter reads endpoint, headers, timeout from OTEL_EXPORTER_OTLP_*.
        out.push(new BatchSpanProcessor({ exporter: new OTLPTraceExporter() }));
      } else {
        console.warn('[tracing] OTEL_TRACES_EXPORTER=otlp but no OTEL_EXPORTER_OTLP_ENDPOINT set; spans are dropped');
      }
    } else if (name === 'console') {
      out.push(new BatchSpanProcessor({ exporter: new ConsoleSpanExporter() }));
    } else {
      console.warn(`[tracing] unsupported OTEL_TRACES_EXPORTER entry "${name}" ignored`);
    }
  }
  return out;
}

const sdkConfig = {
  spanProcessors: [new TestIdSpanProcessor(env('HIVE_RUN_ID')), ...exporterProcessors()],
  // Traces only. NodeSDK would otherwise build OTLP metric/log exporters that
  // fall back to an implicit localhost endpoint and stall shutdown flushing it.
  metricReaders: [],
  logRecordProcessors: [],
  instrumentations: [
    getNodeAutoInstrumentations({
      // Consumer spans are created by hand in index.js (`billing process`),
      // with the context extracted from the message headers.
      '@opentelemetry/instrumentation-kafkajs': { enabled: false },
      '@opentelemetry/instrumentation-fs': { enabled: false },
      '@opentelemetry/instrumentation-dns': { enabled: false },
      '@opentelemetry/instrumentation-net': { enabled: false },
      '@opentelemetry/instrumentation-http': {
        ignoreIncomingRequestHook: (req) => (req.url || '').startsWith('/healthz'),
      },
    }),
  ],
};
// Explicit W3C composite unless the operator chose propagators via env.
if (!env('OTEL_PROPAGATORS')) {
  sdkConfig.textMapPropagator = new CompositePropagator({
    propagators: [new W3CTraceContextPropagator(), new W3CBaggagePropagator()],
  });
}

const sdk = new NodeSDK(sdkConfig);
sdk.start();

let shutdownPromise;
function shutdown() {
  if (!shutdownPromise) {
    shutdownPromise = sdk.shutdown().catch((err) => {
      console.error('[tracing] shutdown failed', err);
    });
  }
  return shutdownPromise;
}

module.exports = { sdk, shutdown };
