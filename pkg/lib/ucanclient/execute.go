package ucanclient

import (
	"context"
	"fmt"
	"reflect"

	"github.com/alanshaw/ucantone/client"
	edm "github.com/alanshaw/ucantone/errors/datamodel"
	"github.com/alanshaw/ucantone/execution"
	"github.com/alanshaw/ucantone/ipld"
	"github.com/alanshaw/ucantone/ipld/codec/dagcbor"
	"github.com/alanshaw/ucantone/ipld/datamodel"
	"github.com/alanshaw/ucantone/result"
	"github.com/alanshaw/ucantone/ucan"
	"go.uber.org/zap"
)

// Execute sends the given invocation using the provided client and decodes the
// response into the specified type.
func Execute[T dagcbor.Unmarshaler](
	ctx context.Context,
	client *client.HTTPClient,
	logger *zap.Logger,
	inv ucan.Invocation,
	options ...execution.RequestOption,
) (T, ucan.Receipt, error) {
	var zero T
	resp, err := client.Execute(execution.NewRequest(ctx, inv, options...))
	if err != nil {
		return zero, nil, fmt.Errorf("executing invocation: %w", err)
	}

	rcpt := resp.Receipt()
	ok, err := result.MatchResultR2(
		rcpt.Out(),
		func(o ipld.Any) (T, error) {
			var ok T
			// if ok is a pointer type, then we need to create an instance of it
			// because rebind requires a non-nil pointer.
			typ := reflect.TypeOf(ok)
			if typ.Kind() == reflect.Ptr {
				ok = reflect.New(typ.Elem()).Interface().(T)
			}
			err := datamodel.Rebind(datamodel.NewAny(o), ok)
			if err != nil {
				return zero, fmt.Errorf("binding invocation response: %w", err)
			}
			return ok, nil
		},
		func(x ipld.Any) (T, error) {
			var model edm.ErrorModel
			err := datamodel.Rebind(datamodel.NewAny(x), &model)
			if err != nil {
				logger.Error("execution failure", zap.Any("error", x))
				return zero, fmt.Errorf("executing invocation: %v", x)
			}
			logger.Error("execution failure", zap.String("name", model.ErrorName), zap.Error(model))
			return zero, fmt.Errorf("executing invocation: %w", model)
		},
	)
	return ok, rcpt, err
}
