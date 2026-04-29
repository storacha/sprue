package handlers

import (
	"context"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/storacha/go-ucanto/core/invocation"
	"github.com/storacha/go-ucanto/core/ipld"
	"github.com/storacha/go-ucanto/core/receipt/fx"
	"github.com/storacha/go-ucanto/core/result"
	"github.com/storacha/go-ucanto/core/result/failure"
	"github.com/storacha/go-ucanto/server"
	"github.com/storacha/go-ucanto/ucan"
	"github.com/storacha/go-ucanto/validator"
)

const handlerInstrumentationName = "github.com/storacha/sprue/pkg/service/handlers"

var handlerTracer = otel.Tracer(handlerInstrumentationName)

// Traced wraps a server.HandlerFunc so that each invocation opens a child
// span named "ucan.<ability>" carrying the UCAN resource, issuer, audience,
// and invocation CID as attributes. The span records the transport-level
// error returned by the handler; domain-level failures (encoded in the
// result.Result failure branch) are left for the caller to interpret.
func Traced[C any, O ipld.Builder, X failure.IPLDBuilderFailure](
	ability string,
	handler server.HandlerFunc[C, O, X],
) server.HandlerFunc[C, O, X] {
	return func(
		ctx context.Context,
		cap ucan.Capability[C],
		inv invocation.Invocation,
		ictx server.InvocationContext,
	) (result.Result[O, X], fx.Effects, error) {
		ctx, span := handlerTracer.Start(ctx, "ucan."+ability,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(
				attribute.String("ucan.ability", ability),
				attribute.String("ucan.resource", cap.With()),
				attribute.String("ucan.issuer", inv.Issuer().DID().String()),
				attribute.String("ucan.audience", inv.Audience().DID().String()),
				attribute.String("invocation.cid", inv.Link().String()),
			),
		)
		defer span.End()

		res, effects, err := handler(ctx, cap, inv, ictx)
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "handler error")
		}
		return res, effects, err
	}
}

// ProvideTraced is a drop-in replacement for server.Provide that wraps the
// handler in Traced. The ability string is passed explicitly so the span
// name is built without reflecting over the CapabilityParser.
func ProvideTraced[C any, O ipld.Builder, X failure.IPLDBuilderFailure](
	capability validator.CapabilityParser[C],
	ability string,
	handler server.HandlerFunc[C, O, X],
) server.ServiceMethod[O, failure.IPLDBuilderFailure] {
	return server.Provide(capability, Traced(ability, handler))
}
