package client

import (
	"context"
	"fmt"
	"net/url"
	"slices"

	ucanlib "github.com/alanshaw/libracha/ucan"
	"github.com/alanshaw/ucantone/client"
	"github.com/alanshaw/ucantone/did"
	"github.com/alanshaw/ucantone/execution"
	"github.com/alanshaw/ucantone/ucan"
	"github.com/alanshaw/ucantone/ucan/invocation"
	providercap "github.com/storacha/sprue/pkg/capabilities/admin/provider"
	weightcap "github.com/storacha/sprue/pkg/capabilities/admin/provider/weight"
	"github.com/storacha/sprue/pkg/lib/ucanclient"
	"go.uber.org/zap"
)

type Client struct {
	uploadServiceID did.DID
	client          *client.HTTPClient
	signer          ucan.Signer
	logger          *zap.Logger
}

func New(uploadServiceID did.DID, endpoint *url.URL, signer ucan.Signer, logger *zap.Logger) (*Client, error) {
	client, err := client.NewHTTP(endpoint)
	if err != nil {
		return nil, fmt.Errorf("creating HTTP client: %w", err)
	}
	return NewWithClient(uploadServiceID, client, signer, logger), nil
}

func NewWithClient(uploadServiceID did.DID, client *client.HTTPClient, signer ucan.Signer, logger *zap.Logger) *Client {
	return &Client{
		uploadServiceID: uploadServiceID,
		signer:          signer,
		client:          client,
		logger:          logger,
	}
}

func (c *Client) AdminProviderRegister(ctx context.Context, providerID did.DID, endpoint string, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Receipt, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.uploadServiceID, providercap.RegisterCommand, c.uploadServiceID)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.uploadServiceID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := providercap.Register.Invoke(
		c.signer,
		c.uploadServiceID,
		&providercap.RegisterArguments{
			Provider: providerID,
			Endpoint: endpoint,
		},
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("invoking provider register: %w", err)
	}

	_, rcpt, err := ucanclient.Execute[*providercap.RegisterOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, fmt.Errorf("executing provider register invocation: %w", err)
	}
	return rcpt, nil
}

func (c *Client) AdminProviderDeregister(ctx context.Context, providerID did.DID, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Receipt, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.uploadServiceID, providercap.DeregisterCommand, c.uploadServiceID)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.uploadServiceID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := providercap.Deregister.Invoke(
		c.signer,
		c.uploadServiceID,
		&providercap.DeregisterArguments{
			Provider: providerID,
		},
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("invoking provider deregister: %w", err)
	}

	_, rcpt, err := ucanclient.Execute[*providercap.DeregisterOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, fmt.Errorf("executing provider deregister invocation: %w", err)
	}
	return rcpt, nil
}

func (c *Client) AdminProviderList(ctx context.Context, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (*providercap.ListOK, ucan.Receipt, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.uploadServiceID, providercap.ListCommand, c.uploadServiceID)
	if err != nil {
		return nil, nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.uploadServiceID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := providercap.List.Invoke(
		c.signer,
		c.uploadServiceID,
		&providercap.ListArguments{},
		options...,
	)
	if err != nil {
		return nil, nil, fmt.Errorf("invoking provider list: %w", err)
	}

	listOK, rcpt, err := ucanclient.Execute[*providercap.ListOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, nil, fmt.Errorf("executing provider list invocation: %w", err)
	}
	return listOK, rcpt, nil
}

func (c *Client) AdminProviderWeightSet(ctx context.Context, signer ucan.Signer, providerID did.DID, weight int, replicationWeight int, matcher ucanlib.DelegationMatcher, options ...invocation.Option) (ucan.Receipt, error) {
	prfs, prfLinks, err := ucanlib.ProofChain(ctx, matcher, c.uploadServiceID, weightcap.SetCommand, c.uploadServiceID)
	if err != nil {
		return nil, fmt.Errorf("building proof chain: %w", err)
	}

	options = slices.Clone(options)
	options = append(
		options,
		invocation.WithAudience(c.uploadServiceID),
		invocation.WithProofs(prfLinks...),
	)

	inv, err := weightcap.Set.Invoke(
		c.signer,
		c.uploadServiceID,
		&weightcap.SetArguments{
			Provider:          providerID,
			Weight:            int64(weight),
			ReplicationWeight: int64(replicationWeight),
		},
		options...,
	)
	if err != nil {
		return nil, fmt.Errorf("invoking provider weight set: %w", err)
	}

	_, rcpt, err := ucanclient.Execute[*weightcap.SetOK](ctx, c.client, c.logger, inv, execution.WithProofs(prfs...))
	if err != nil {
		return nil, fmt.Errorf("executing provider weight set invocation: %w", err)
	}
	return rcpt, nil
}
