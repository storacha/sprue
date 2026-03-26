package fx

import (
	"fmt"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/identity"
	"github.com/storacha/sprue/pkg/mailer"
	"github.com/storacha/sprue/pkg/mailer/nop"
	"github.com/storacha/sprue/pkg/mailer/postmark"
	"github.com/storacha/sprue/pkg/provisioner"
	"github.com/storacha/sprue/pkg/store/consumer"
	"github.com/storacha/sprue/pkg/store/customer"
	"github.com/storacha/sprue/pkg/store/subscription"
)

var ServicesModule = fx.Module("services",
	fx.Provide(NewMailingService),
	fx.Provide(NewProvisioningService),
)

func NewMailingService(deploymentCfg config.DeploymentConfig, mailerCfg config.MailerConfig, logger *zap.Logger) (mailer.Mailer, error) {
	switch mailerCfg.Type {
	case "nop":
		return nop.New(logger), nil
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
	customerStore customer.Store,
	consumerStore consumer.Store,
	subscriptionStore subscription.Store,
) *provisioner.ProvisioningService {
	return provisioner.New([]provisioner.ServiceDID{id.Signer.DID()}, customerStore, consumerStore, subscriptionStore)
}
