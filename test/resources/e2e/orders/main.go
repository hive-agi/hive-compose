// Command orders is the hive-compose e2e fixture "project A": an HTTP
// service that accepts orders and publishes them to Kafka, traced with
// OpenTelemetry configured purely from the standard OTEL_* env vars.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const failMsg = "sku boom rejected"

// Order is the wire shape accepted on POST /orders and published to Kafka.
type Order struct {
	ID  string `json:"id"`
	SKU string `json:"sku"`
	Qty int    `json:"qty"`
}

func port() string {
	if p := os.Getenv("PORT"); p != "" {
		return p
	}
	return "8080"
}

// attrProcessor copies baggage member test.id onto every span at start, as
// test.id and hive.test.id (the hive-compose otel capability's name), plus
// env HIVE_RUN_ID as hive.run.id.
type attrProcessor struct{ runID string }

func (p attrProcessor) OnStart(ctx context.Context, s sdktrace.ReadWriteSpan) {
	if m := baggage.FromContext(ctx).Member("test.id"); m.Value() != "" {
		s.SetAttributes(attribute.String("test.id", m.Value()),
			attribute.String("hive.test.id", m.Value()))
	}
	if p.runID != "" {
		s.SetAttributes(attribute.String("hive.run.id", p.runID))
	}
}
func (attrProcessor) OnEnd(sdktrace.ReadOnlySpan)      {}
func (attrProcessor) Shutdown(context.Context) error   { return nil }
func (attrProcessor) ForceFlush(context.Context) error { return nil }

func initTracing(ctx context.Context) (*sdktrace.TracerProvider, error) {
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{}, propagation.Baggage{}))

	res, err := resource.New(ctx,
		resource.WithFromEnv(), // OTEL_SERVICE_NAME, OTEL_RESOURCE_ATTRIBUTES
		resource.WithTelemetrySDK(),
	)
	if errors.Is(err, resource.ErrPartialResource) {
		log.Printf("resource: %v (continuing with the attributes that parsed)", err)
	} else if err != nil {
		return nil, err
	}
	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(attrProcessor{runID: os.Getenv("HIVE_RUN_ID")}),
	}
	// Exporter is configured only from the standard OTEL_EXPORTER_OTLP_* env
	// vars. Without an endpoint we run with no exporter (spans are dropped).
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT") != "" {
		exp, err := otlptracehttp.New(ctx)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdktrace.WithBatcher(exp))
	}
	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)
	return tp, nil
}

// headerCarrier adapts kafka message headers to a propagation.TextMapCarrier.
type headerCarrier struct{ msg *kafka.Message }

func (c headerCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (c headerCarrier) Set(key, val string) {
	for i, h := range c.msg.Headers {
		if strings.EqualFold(h.Key, key) {
			c.msg.Headers[i].Value = []byte(val)
			return
		}
	}
	c.msg.Headers = append(c.msg.Headers, kafka.Header{Key: key, Value: []byte(val)})
}

func (c headerCarrier) Keys() []string {
	ks := make([]string, 0, len(c.msg.Headers))
	for _, h := range c.msg.Headers {
		ks = append(ks, h.Key)
	}
	return ks
}

// retriable reports the errors a write to a topic that is still being
// auto-created answers with, also when wrapped in a batch's WriteErrors.
func retriable(err error) bool {
	if err == nil {
		return false
	}
	var we kafka.WriteErrors
	if errors.As(err, &we) {
		for _, e := range we {
			if e != nil && !retriable(e) {
				return false
			}
		}
		return true
	}
	return errors.Is(err, kafka.UnknownTopicOrPartition) || errors.Is(err, kafka.LeaderNotAvailable)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

type server struct {
	writer *kafka.Writer
	tracer trace.Tracer
}

func (s *server) orders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}
	ctx := r.Context()
	span := trace.SpanFromContext(ctx)

	forced := r.Header.Get("X-Fail") == "1"
	var o Order
	if err := json.NewDecoder(r.Body).Decode(&o); err != nil && !forced {
		span.RecordError(err)
		span.SetStatus(codes.Error, "bad request")
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid order json"})
		return
	}
	span.SetAttributes(
		attribute.String("order.id", o.ID),
		attribute.String("order.sku", o.SKU),
		attribute.Int("order.qty", o.Qty),
	)

	// The rejection is recorded on its own child span: otelhttp overwrites the
	// server span's status with (Error, "") for a 5xx once the handler returns,
	// which would erase the message `why` looks for.
	_, vspan := s.tracer.Start(ctx, "orders validate")
	if forced || o.SKU == "boom" {
		vspan.RecordError(errors.New(failMsg))
		vspan.SetStatus(codes.Error, failMsg)
		vspan.End()
		span.RecordError(errors.New(failMsg))
		span.SetStatus(codes.Error, failMsg)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": failMsg})
		return
	}
	vspan.End()

	body, _ := json.Marshal(o)
	pctx, pspan := s.tracer.Start(ctx, s.writer.Topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", s.writer.Topic),
			attribute.String("messaging.operation.type", "publish"),
		))
	msg := kafka.Message{Key: []byte(o.ID), Value: body}
	otel.GetTextMapPropagator().Inject(pctx, headerCarrier{&msg})

	wctx, cancel := context.WithTimeout(pctx, 10*time.Second)
	defer cancel()
	err := s.writer.WriteMessages(wctx, msg)
	// The first write to a fresh topic races its auto-creation; the broker
	// answers UnknownTopicOrPartition until the leader is elected. Retry that
	// one error for a bounded time, then give up.
	for i := 0; i < 20 && retriable(err) && wctx.Err() == nil; i++ {
		time.Sleep(250 * time.Millisecond)
		err = s.writer.WriteMessages(wctx, msg)
	}
	if err != nil {
		pspan.RecordError(err)
		pspan.SetStatus(codes.Error, "kafka publish failed")
		pspan.End()
		span.RecordError(err)
		span.SetStatus(codes.Error, "kafka publish failed")
		log.Printf("publish failed: %v", err)
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "kafka publish failed"})
		return
	}
	pspan.End()
	writeJSON(w, http.StatusAccepted, o)
}

func healthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "text/plain")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("ok\n"))
}

func healthcheck() int {
	c := http.Client{Timeout: 3 * time.Second}
	resp, err := c.Get("http://127.0.0.1:" + port() + "/healthz")
	if err != nil {
		return 1
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}

func main() {
	hc := flag.Bool("healthcheck", false, "GET /healthz on localhost and exit 0 if healthy, 1 otherwise")
	flag.Parse()
	if *hc {
		os.Exit(healthcheck())
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	tp, err := initTracing(ctx)
	if err != nil {
		log.Fatalf("tracing init: %v", err)
	}

	bootstrap := os.Getenv("KAFKA_BOOTSTRAP")
	if bootstrap == "" {
		bootstrap = "localhost:9092"
	}
	topic := os.Getenv("TOPIC_PREFIX") + "orders"
	writer := &kafka.Writer{
		Addr:                   kafka.TCP(strings.Split(bootstrap, ",")...),
		Topic:                  topic,
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		RequiredAcks:           kafka.RequireOne,
		BatchTimeout:           10 * time.Millisecond,
	}

	s := &server{writer: writer, tracer: otel.Tracer("orders")}
	mux := http.NewServeMux()
	mux.HandleFunc("/orders", s.orders)
	mux.HandleFunc("/healthz", healthz)

	srv := &http.Server{
		Addr:              ":" + port(),
		Handler:           otelhttp.NewHandler(mux, "orders", otelhttp.WithSpanNameFormatter(func(_ string, r *http.Request) string { return r.Method + " " + r.URL.Path })),
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("orders listening on %s (kafka=%s topic=%s)", srv.Addr, bootstrap, topic)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("listen: %v", err)
		}
	}()

	<-ctx.Done()
	log.Printf("shutting down")
	sctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = srv.Shutdown(sctx)
	_ = writer.Close()
	if err := tp.ForceFlush(sctx); err != nil {
		log.Printf("tracer flush: %v", err)
	}
	if err := tp.Shutdown(sctx); err != nil {
		log.Printf("tracer shutdown: %v", err)
	}
}
