package fx

import (
	"fmt"

	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/storacha/sprue/internal/config"
	"github.com/storacha/sprue/pkg/mailer"
	"github.com/storacha/sprue/pkg/mailer/nop"
	"github.com/storacha/sprue/pkg/mailer/postmark"
)

var MailerModule = fx.Module("mailer",
	fx.Provide(NewMailer),
)

func NewMailer(deploymentCfg config.DeploymentConfig, mailerCfg config.MailerConfig, logger *zap.Logger) (mailer.Mailer, error) {
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
