package piriclient

import (
	"github.com/ipld/go-ipld-prime/datamodel"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
)

const instrumentationName = "github.com/storacha/sprue/pkg/piriclient"

var (
	tracer = otel.Tracer(instrumentationName)
	meter  = otel.Meter(instrumentationName)

	allocateDuration        metric.Float64Histogram
	allocateRequests        metric.Int64Counter
	acceptDuration          metric.Float64Histogram
	acceptRequests          metric.Int64Counter
	replicaAllocateDuration metric.Float64Histogram
	replicaAllocateRequests metric.Int64Counter
)

func init() {
	var err error
	if allocateDuration, err = meter.Float64Histogram(
		"piriclient.allocate.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of blob/allocate invocations to a Piri node."),
	); err != nil {
		otel.Handle(err)
	}
	if allocateRequests, err = meter.Int64Counter(
		"piriclient.allocate.requests",
		metric.WithDescription("Count of blob/allocate invocations, labeled by outcome."),
	); err != nil {
		otel.Handle(err)
	}
	if acceptDuration, err = meter.Float64Histogram(
		"piriclient.accept.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of blob/accept invocations to a Piri node."),
	); err != nil {
		otel.Handle(err)
	}
	if acceptRequests, err = meter.Int64Counter(
		"piriclient.accept.requests",
		metric.WithDescription("Count of blob/accept invocations, labeled by outcome."),
	); err != nil {
		otel.Handle(err)
	}
	if replicaAllocateDuration, err = meter.Float64Histogram(
		"piriclient.replica_allocate.duration",
		metric.WithUnit("s"),
		metric.WithDescription("Duration of blob/replica/allocate invocations to a Piri node."),
	); err != nil {
		otel.Handle(err)
	}
	if replicaAllocateRequests, err = meter.Int64Counter(
		"piriclient.replica_allocate.requests",
		metric.WithDescription("Count of blob/replica/allocate invocations, labeled by outcome."),
	); err != nil {
		otel.Handle(err)
	}
}

// extractErrorDetails pulls a human-readable message out of a receipt error
// node, falling back through "message" → "name" → a static sentinel.
func extractErrorDetails(errNode datamodel.Node) string {
	if msgNode, lookupErr := errNode.LookupByString("message"); lookupErr == nil {
		if msg, asErr := msgNode.AsString(); asErr == nil && msg != "" {
			return msg
		}
	}
	if nameNode, lookupErr := errNode.LookupByString("name"); lookupErr == nil {
		if name, asErr := nameNode.AsString(); asErr == nil && name != "" {
			return name
		}
	}
	return "unknown error"
}
