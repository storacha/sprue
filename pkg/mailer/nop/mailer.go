package nop

import (
	"context"
	"net/url"

	"go.uber.org/zap"
)

// Mailer is a mailer implementation for use in tests and local development.
// It does not send any emails, instead it logs the recipient address and
// validation URL.
type Mailer struct {
	logger *zap.Logger
}

func New(logger *zap.Logger) *Mailer {
	return &Mailer{logger: logger}
}

func (m *Mailer) SendValidation(_ context.Context, to string, validationURL url.URL) error {
	m.logger.Info("Validation email",
		zap.String("to", to),
		zap.String("url", validationURL.String()),
	)
	return nil
}
