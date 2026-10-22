// Package telemetry sends traces and the Prometheus metrics over OTLP, only when
// OTEL_EXPORTER_OTLP_ENDPOINT is set. The standard OTEL_* variables configure the exporters.
package telemetry

import (
	"context"
	"encoding/json"
	"os"
	"regexp"
	"time"

	"github.com/bigbluebutton/bbb-webrtc-recorder/internal/pubsub/events"
	log "github.com/sirupsen/logrus"
	"go.opentelemetry.io/contrib/bridges/otellogrus"
	prombridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

const serviceName = "bbb-webrtc-recorder"

var (
	Tracer = otel.Tracer(serviceName)

	userIdPattern = regexp.MustCompile(`^[a-z]_[a-z0-9]+$`)
	shutdowns     []func(context.Context) error
)

func Enabled() bool {
	return os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" && os.Getenv("OTEL_SDK_DISABLED") != "true"
}

func Start(ctx context.Context) {
	if !Enabled() {
		return
	}

	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES override the defaults
	res, err := resource.New(ctx,
		resource.WithAttributes(attribute.String("service.name", serviceName)),
		resource.WithFromEnv(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
	)
	if err != nil {
		log.Warnf("OpenTelemetry resource: %v", err)
	}

	traceExporter, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Errorf("OpenTelemetry traces disabled: %v", err)
		return
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(traceExporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	shutdowns = append(shutdowns, tp.Shutdown)

	if os.Getenv("OTEL_LOGS_EXPORTER") == "otlp" {
		startLogs(ctx, res)
	}

	if os.Getenv("OTEL_METRICS_EXPORTER") != "none" {
		startMetrics(ctx, res)
	}

	log.Infof("OpenTelemetry export to %s", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
}

func startMetrics(ctx context.Context, res *resource.Resource) {
	metricExporter, err := otlpmetrichttp.New(ctx)
	if err != nil {
		log.Errorf("OpenTelemetry metrics disabled: %v", err)
		return
	}
	reader := sdkmetric.NewPeriodicReader(metricExporter, sdkmetric.WithProducer(prombridge.NewMetricProducer()))
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader), sdkmetric.WithResource(res))
	otel.SetMeterProvider(mp)
	shutdowns = append(shutdowns, mp.Shutdown)
}

func startLogs(ctx context.Context, res *resource.Resource) {
	logExporter, err := otlploghttp.New(ctx)
	if err != nil {
		log.Errorf("OpenTelemetry logs disabled: %v", err)
		return
	}
	lp := sdklog.NewLoggerProvider(sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)), sdklog.WithResource(res))
	log.AddHook(otellogrus.NewHook(serviceName, otellogrus.WithLoggerProvider(lp)))
	shutdowns = append(shutdowns, lp.Shutdown)
}

func Shutdown() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for _, shutdown := range shutdowns {
		if err := shutdown(ctx); err != nil {
			log.Warnf("OpenTelemetry shutdown: %v", err)
		}
	}
}

// StartRequestSpan continues the requester's trace from an optional top-level traceparent field.
func StartRequestSpan(ctx context.Context, message []byte, event *events.Event) (context.Context, trace.Span) {
	// The status polled every few seconds is traced only with OTEL_INSTRUMENTATION_BBB_HEALTH_CHECKS_ENABLED=true
	if !Enabled() || event == nil || (event.Id == "getRecorderStatus" && os.Getenv("OTEL_INSTRUMENTATION_BBB_HEALTH_CHECKS_ENABLED") != "true") {
		return ctx, trace.SpanFromContext(ctx)
	}

	var carrier struct {
		Traceparent string `json:"traceparent"`
	}
	if json.Unmarshal(message, &carrier) == nil && carrier.Traceparent != "" {
		ctx = otel.GetTextMapPropagator().Extract(ctx, propagation.MapCarrier{"traceparent": carrier.Traceparent})
	}

	ctx, span := Tracer.Start(ctx, "process "+event.Id, trace.WithSpanKind(trace.SpanKindConsumer))
	if e := event.StartRecording(); e != nil {
		if e.AdapterOptions != nil && e.AdapterOptions.LiveKit != nil && e.AdapterOptions.LiveKit.Room != "" {
			span.SetAttributes(attribute.String("bbb.meeting.id", e.AdapterOptions.LiveKit.Room))
		}
		if userId, ok := e.Metadata["participantId"].(string); ok && userIdPattern.MatchString(userId) {
			span.SetAttributes(attribute.String("bbb.user.id", userId))
		}
	}

	return ctx, span
}
