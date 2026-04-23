package fx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
)

const (
	telemetryServiceName    = "sprue"
	telemetryShutdownBudget = 15 * time.Second
)

// TelemetryModule sets up OpenTelemetry traces and metrics. The module
// installs the global text-map propagator and OTel error handler
// unconditionally; TracerProvider and MeterProvider are installed only when
// their corresponding Endpoint is configured.
var TelemetryModule = fx.Module("telemetry",
	fx.Provide(NewTelemetryProvider),
	fx.Invoke(RegisterTelemetryLifecycle),
)

// TelemetryProvider owns the SDK providers installed as OTel globals so they
// can be flushed and shut down on application stop.
type TelemetryProvider struct {
	tp *sdktrace.TracerProvider
	mp *sdkmetric.MeterProvider
}

// NewTelemetryProvider builds the OTLP/HTTP exporters and installs them as
// the global TracerProvider and MeterProvider. Either provider may be nil
// when the corresponding endpoint is empty.
func NewTelemetryProvider(
	cfg config.TelemetryConfig,
	dep config.DeploymentConfig,
	logger *zap.Logger,
) (*TelemetryProvider, error) {
	// Always set a propagator so inbound traceparent headers are honoured
	// once downstream instrumentation is wired up, even if we don't export.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	// Route internal OTel SDK errors through zap so exporter failures are
	// visible in structured logs rather than landing on stderr.
	otel.SetErrorHandler(&zapErrorHandler{logger: logger})

	res, err := buildResource(dep)
	if err != nil {
		return nil, fmt.Errorf("building telemetry resource: %w", err)
	}

	t := &TelemetryProvider{}

	if cfg.Traces.Endpoint != "" {
		tp, err := newTracerProvider(cfg.Traces, res)
		if err != nil {
			return nil, fmt.Errorf("tracer provider: %w", err)
		}
		otel.SetTracerProvider(tp)
		t.tp = tp
		logger.Info("telemetry traces enabled",
			zap.String("endpoint", cfg.Traces.Endpoint),
			zap.Float64("sample_ratio", cfg.Traces.SampleRatio),
		)
	} else {
		logger.Info("telemetry traces disabled")
	}

	if cfg.Metrics.Endpoint != "" {
		mp, err := newMeterProvider(cfg.Metrics, res)
		if err != nil {
			return nil, fmt.Errorf("meter provider: %w", err)
		}
		otel.SetMeterProvider(mp)
		t.mp = mp
		logger.Info("telemetry metrics enabled",
			zap.String("endpoint", cfg.Metrics.Endpoint),
			zap.Duration("export_interval", cfg.Metrics.ExportInterval),
		)
	} else {
		logger.Info("telemetry metrics disabled")
	}

	return t, nil
}

// RegisterTelemetryLifecycle hooks provider shutdown into the fx lifecycle.
// The telemetry module is registered right after LoggerModule in AppModule,
// so its OnStop runs after the HTTP server's — giving any last request
// spans a chance to flush before the exporter closes.
func RegisterTelemetryLifecycle(lc fx.Lifecycle, t *TelemetryProvider, logger *zap.Logger) {
	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			if t.tp == nil && t.mp == nil {
				return nil
			}
			logger.Info("flushing telemetry providers")
			shutdownCtx, cancel := context.WithTimeout(ctx, telemetryShutdownBudget)
			defer cancel()
			var errs []error
			if t.tp != nil {
				if err := t.tp.Shutdown(shutdownCtx); err != nil {
					errs = append(errs, fmt.Errorf("tracer provider shutdown: %w", err))
				}
			}
			if t.mp != nil {
				if err := t.mp.Shutdown(shutdownCtx); err != nil {
					errs = append(errs, fmt.Errorf("meter provider shutdown: %w", err))
				}
			}
			return errors.Join(errs...)
		},
	})
}

func buildResource(dep config.DeploymentConfig) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{semconv.ServiceName(telemetryServiceName)}
	if dep.Environment != "" {
		attrs = append(attrs, semconv.DeploymentEnvironment(dep.Environment))
	}
	return resource.Merge(resource.Default(), resource.NewSchemaless(attrs...))
}

func newTracerProvider(cfg config.TracesConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlptracehttp.WithHeaders(cfg.Headers))
	}
	if cfg.Timeout > 0 {
		opts = append(opts, otlptracehttp.WithTimeout(cfg.Timeout))
	}
	if comp, err := parseTraceCompression(cfg.Compression); err != nil {
		return nil, err
	} else if cfg.Compression != "" {
		opts = append(opts, otlptracehttp.WithCompression(comp))
	}

	exp, err := otlptrace.New(context.Background(), otlptracehttp.NewClient(opts...))
	if err != nil {
		return nil, fmt.Errorf("creating otlp trace exporter: %w", err)
	}

	// Parent-only sampling: a request with no upstream traceparent is not
	// traced by default. This silences synthetic traffic like health-check
	// probes and forces sampling decisions to flow from instrumented callers.
	// SampleRatio lets operators opt in to probabilistic root sampling when
	// they want unparented traffic recorded anyway (e.g. in development).
	root := sdktrace.NeverSample()
	if cfg.SampleRatio > 0 {
		root = sdktrace.TraceIDRatioBased(cfg.SampleRatio)
	}

	return sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exp),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(root)),
	), nil
}

func newMeterProvider(cfg config.MetricsConfig, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	opts := []otlpmetrichttp.Option{otlpmetrichttp.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		opts = append(opts, otlpmetrichttp.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(cfg.Headers))
	}
	if cfg.Timeout > 0 {
		opts = append(opts, otlpmetrichttp.WithTimeout(cfg.Timeout))
	}
	if comp, err := parseMetricCompression(cfg.Compression); err != nil {
		return nil, err
	} else if cfg.Compression != "" {
		opts = append(opts, otlpmetrichttp.WithCompression(comp))
	}

	exp, err := otlpmetrichttp.New(context.Background(), opts...)
	if err != nil {
		return nil, fmt.Errorf("creating otlp metric exporter: %w", err)
	}

	readerOpts := []sdkmetric.PeriodicReaderOption{}
	if cfg.ExportInterval > 0 {
		readerOpts = append(readerOpts, sdkmetric.WithInterval(cfg.ExportInterval))
	}
	reader := sdkmetric.NewPeriodicReader(exp, readerOpts...)

	return sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	), nil
}

func parseTraceCompression(s string) (otlptracehttp.Compression, error) {
	switch s {
	case "", "none":
		return otlptracehttp.NoCompression, nil
	case "gzip":
		return otlptracehttp.GzipCompression, nil
	default:
		return 0, fmt.Errorf("unsupported traces compression %q (valid: none, gzip)", s)
	}
}

func parseMetricCompression(s string) (otlpmetrichttp.Compression, error) {
	switch s {
	case "", "none":
		return otlpmetrichttp.NoCompression, nil
	case "gzip":
		return otlpmetrichttp.GzipCompression, nil
	default:
		return 0, fmt.Errorf("unsupported metrics compression %q (valid: none, gzip)", s)
	}
}

// zapErrorHandler adapts otel.ErrorHandler to zap.
type zapErrorHandler struct {
	logger *zap.Logger
}

func (h *zapErrorHandler) Handle(err error) {
	if err == nil {
		return
	}
	h.logger.Warn("opentelemetry error", zap.Error(err))
}
