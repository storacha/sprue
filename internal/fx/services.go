package fx

import (
	"fmt"
	"net/smtp"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/billing"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/mailer"
	"github.com/storacha/sprue/pkg/mailer/nop"
	"github.com/storacha/sprue/pkg/mailer/postmark"
	smtp_mailer "github.com/storacha/sprue/pkg/mailer/smtp"
	"github.com/storacha/sprue/pkg/provisioning"
	"github.com/storacha/sprue/pkg/routing"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/subscription"
)

var ServicesModule = fx.Module("services",
	fx.Provide(NewMailingService),
	fx.Provide(NewProvisioningService),
	fx.Provide(billing.NewService),
	fx.Provide(routing.NewService),
)

func NewMailingService(deploymentCfg config.DeploymentConfig, mailerCfg config.MailerConfig, logger *zap.Logger) (mailer.Mailer, error) {
	switch mailerCfg.Type {
	case "nop":
		return nop.New(logger), nil
	case "smtp":
		if mailerCfg.SMTPAddr == "" {
			return nil, fmt.Errorf("missing SMTP mailer address")
		}
		if mailerCfg.SMTPAuthUser == "" {
			return nil, fmt.Errorf("missing SMTP mailer username")
		}
		if mailerCfg.SMTPAuthSecret == "" {
			return nil, fmt.Errorf("missing SMTP mailer CRAMMD5 auth secret")
		}
		auth := smtp.CRAMMD5Auth(mailerCfg.SMTPAuthUser, mailerCfg.SMTPAuthSecret)
		return smtp_mailer.New(
			mailerCfg.SMTPAddr,
			auth,
			smtp_mailer.WithSubject(mailerCfg.Subject),
			smtp_mailer.WithSender(mailerCfg.Sender),
		), nil
	case "postmark":
		if mailerCfg.PostmarkToken == "" {
			return nil, fmt.Errorf("postmark mail configured but token not set")
		}
		postmarkOpts := []postmark.Option{postmark.WithEnvironment(deploymentCfg.Environment)}
		if mailerCfg.Sender != "" {
			postmarkOpts = append(postmarkOpts, postmark.WithSender(mailerCfg.Sender))
		}
		return postmark.New(mailerCfg.PostmarkToken, postmarkOpts...), nil
	}
	logger.Warn("Unknown mailer, using no-op mailer", zap.String("type", mailerCfg.Type))
	return nop.New(logger), nil
}

func NewProvisioningService(
	id *identity.Identity,
	consumerStore consumer.Store,
	subscriptionStore subscription.Store,
) *provisioning.Service {
	return provisioning.NewService([]provisioning.ServiceDID{id.Signer.DID()}, consumerStore, subscriptionStore)
}
