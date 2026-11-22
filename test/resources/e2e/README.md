# E2E fixtures: stacks, shared Kafka, an opt-out, a failure explained

Draft README section for hc-11 (plan `20261122140443-09c5c225`). These are the
compose projects `test/hive_compose/stack_e2e_test.clj` mounts. None of them
knows hive-compose: they integrate only through protocols, namely env vars, a
`/healthz` health check, OTLP traces, W3C `traceparent`/`baggage`, and JUnit
XML or TAP reports.

## The projects

| dir        | role                         | language    | image                          |
|------------|------------------------------|-------------|--------------------------------|
| `kafka/`   | provider of `kafka`          | (apache/kafka:3.9.1, KRaft, one node) | upstream |
| `orders/`  | project A, needs `kafka`     | Go 1.24     | `hive-compose-e2e/orders:dev`  |
| `billing/` | project B (and C), needs `kafka` | Node 22 | `hive-compose-e2e/billing:dev` |
| `suite/`   | suite runner `orders-suite`  | sh + curl   | `hive-compose-e2e/suite:dev`   |

* **orders** serves `POST /orders` (publishes the order JSON to
  `${TOPIC_PREFIX}orders`, trace context injected into the Kafka headers) and
  `GET /healthz`. A request with `X-Fail: 1`, or an order with sku `boom`, is
  rejected: an error span with status message `sku boom rejected` and a 500.
  That is the deliberate failure `compose why` has to find.
* **billing** consumes `${TOPIC_PREFIX}orders`, continues the producer's trace
  per message, and serves `GET /invoices` and `GET /healthz`.
* Both copy the baggage member `test.id` onto every span as the attribute
  `test.id` (and `HIVE_RUN_ID`, when set, as `hive.run.id`).

Every project reads its wiring from the environment, nothing is hardcoded:

| var | meaning |
|-----|---------|
| `KAFKA_BOOTSTRAP` | `host:port` of the resolved Kafka instance |
| `TOPIC_PREFIX` | tenancy prefix so consumers of a shared Kafka do not cross-talk |
| `HIVE_RUN_ID` | the stack run id |
| `OTEL_EXPORTER_OTLP_ENDPOINT`, `OTEL_SERVICE_NAME`, `OTEL_RESOURCE_ATTRIBUTES` | standard OTel SDK config |
| `HIVE_NETWORK` | docker network the run's containers share (default `hive-e2e`, external) |
| `KAFKA_ALIAS` | (kafka only) network alias and advertised host of that instance |

## The scenario

```
            otel (grafana/otel-lgtm)
               ^  OTLP from every service
               |
  A orders ----+---- B billing          C billing (second instance)
        \          /                         |
         kafka  (shared, -p kafka)        kafka (private, -p kafka-c-<run>)
```

Declared as capabilities (sketch, hc-2 schema):

```edn
{"kafka"   {:project/provides {"kafka" {:version "3.9" :service "kafka"}}}
 "orders"  {:project/needs {"kafka" {:version "^3"} "otel" {}}}
 "billing" {:project/needs {"kafka" {:version "^3"} "otel" {}}}
 "billing-c" {:project/dir "test/resources/e2e/billing"
              :project/needs {"kafka" {:version "^3" :share? false} "otel" {}}}}
```

1. A and B need compatible Kafka ranges, so the resolver yields **one** Kafka
   instance under the provider's project name. A produces and B consumes the
   same topic, so both are wired with the run's `TOPIC_PREFIX` (the prefix
   isolates runs on the shared broker, not producer from consumer).
2. C (`billing` again, under another compose project name) says
   `:share? false` and gets a **private** Kafka: `-p kafka-<consumer>-<run>`
   with its own `KAFKA_ALIAS`, so it never sees A's orders.
3. `orders-suite` runs with `TRACEPARENT` and `BAGGAGE` set; per case it sends
   a fresh `traceparent` in that trace and `baggage: test.id=<case>`. It
   writes `junit.xml` (or `results.tap`) to `/reports`: case
   `orders.place_good_order` passes (the order shows up in `GET /invoices`),
   case `orders.place_boom_order` fails on purpose.
4. `compose why test=orders.place_boom_order` returns the span carrying
   `test.id=orders.place_boom_order` with status `sku boom rejected`.

## Running it by hand

```sh
cd test/resources/e2e
docker network create hive-e2e
docker run -d --name lgtm --network hive-e2e -p 3000:3000 grafana/otel-lgtm:0.11.10

docker compose -p kafka -f kafka/compose.yml up -d --wait

export KAFKA_BOOTSTRAP=kafka:9092 TOPIC_PREFIX=run1. HIVE_RUN_ID=run1
export OTEL_EXPORTER_OTLP_ENDPOINT=http://lgtm:4318 OTEL_RESOURCE_ATTRIBUTES=hive.run.id=run1
docker compose -p orders  -f orders/compose.yml  up -d --build --wait
docker compose -p billing -f billing/compose.yml up -d --build --wait

REPORTS_DIR=$PWD/reports docker compose -p suite -f suite/compose.yml run --rm suite   # exits 1
REPORTS_DIR=$PWD/reports docker compose -p suite -f suite/compose.yml --profile tap run --rm suite-tap
```

Then Grafana (`localhost:3000`, Explore, Tempo) answers
`{span.test.id="orders.place_boom_order" && status=error}`.

Tear down with `docker compose -p <name> -f <dir>/compose.yml down` per project,
`docker rm -f lgtm` and `docker network rm hive-e2e`.

## Pins

Every image and dependency is pinned: `apache/kafka:3.9.1`,
`grafana/otel-lgtm:0.11.10`, `golang:1.24.5-alpine3.22`,
`gcr.io/distroless/static-debian12:nonroot@sha256:afa5c872…`,
`node:22.17.1-alpine3.22`, `curlimages/curl:8.11.1`; Go modules by `go.sum`,
npm packages by exact versions and `package-lock.json`.

`clojure -M:integration` builds the three images
(`hive_compose/e2e_fixtures_test.clj`, skipped without docker).
