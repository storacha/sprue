package mailer

import (
	"context"
	"net/url"
)

type Mailer interface {
	// SendValidation sends a validation email to the specified address with the
	// provided validation URL.
	SendValidation(ctx context.Context, to string, validationURL url.URL) error
}
