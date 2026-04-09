package postmark

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, url.URL) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	u, _ := url.Parse(srv.URL)
	return srv, *u
}

func TestNew_Defaults(t *testing.T) {
	m := New("tok")
	require.Equal(t, "tok", m.token)
	require.Equal(t, defaultSender, m.sender)
	require.Empty(t, m.env)
	require.Equal(t, http.DefaultClient, m.client)
}

func TestWithSender(t *testing.T) {
	m := New("tok", WithSender("Custom <custom@example.com>"))
	require.Equal(t, "Custom <custom@example.com>", m.sender)
}

func TestWithSender_Empty(t *testing.T) {
	m := New("tok", WithSender(""))
	require.Equal(t, defaultSender, m.sender)
}

func TestWithEnvironment(t *testing.T) {
	m := New("tok", WithEnvironment("staging"))
	require.Equal(t, "staging", m.env)
}

func TestWithEndpoint(t *testing.T) {
	u, _ := url.Parse("https://example.com/email")
	m := New("tok", WithEndpoint(*u))
	require.Equal(t, u.String(), m.endpoint.String())
}

func TestSendValidation_Success(t *testing.T) {
	var capturedReq struct {
		From          string         `json:"From"`
		To            string         `json:"To"`
		TemplateAlias string         `json:"TemplateAlias"`
		TemplateModel map[string]any `json:"TemplateModel"`
	}
	var capturedToken string

	_, endpoint := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		capturedToken = r.Header.Get("X-Postmark-Server-Token")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &capturedReq)
		w.WriteHeader(http.StatusOK)
	})

	m := New("test-token", WithEndpoint(endpoint))
	validationURL, _ := url.Parse("https://storacha.network/validate?code=abc")
	err := m.SendValidation(context.Background(), "user@example.com", *validationURL)
	require.NoError(t, err)
	require.Equal(t, "test-token", capturedToken)
	require.Equal(t, "user@example.com", capturedReq.To)
	require.Equal(t, defaultSender, capturedReq.From)
	require.Equal(t, template, capturedReq.TemplateAlias)
	require.Equal(t, "user@example.com", capturedReq.TemplateModel["email"])
	require.Equal(t, validationURL.String(), capturedReq.TemplateModel["action_url"])
}

func TestSendValidation_EnvironmentPropagated(t *testing.T) {
	var capturedModel map[string]any

	_, endpoint := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			TemplateModel map[string]any `json:"TemplateModel"`
		}
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &body)
		capturedModel = body.TemplateModel
		w.WriteHeader(http.StatusOK)
	})

	m := New("tok", WithEndpoint(endpoint), WithEnvironment("dev"))
	u, _ := url.Parse("https://storacha.network/validate?code=xyz")
	err := m.SendValidation(context.Background(), "a@b.com", *u)
	require.NoError(t, err)
	require.Equal(t, "dev", capturedModel["environment_name"])
}

func TestSendValidation_NonOKStatusReturnsError(t *testing.T) {
	_, endpoint := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte(`{"Message":"Unauthorized"}`))
	})

	m := New("bad-token", WithEndpoint(endpoint))
	u, _ := url.Parse("https://storacha.network/validate?code=abc")
	err := m.SendValidation(context.Background(), "user@example.com", *u)
	require.Error(t, err)
}

func TestSendValidation_NetworkError(t *testing.T) {
	u, _ := url.Parse("http://127.0.0.1:1") // nothing listening here
	m := New("tok", WithEndpoint(*u))
	validationURL, _ := url.Parse("https://storacha.network/validate?code=abc")
	err := m.SendValidation(context.Background(), "user@example.com", *validationURL)
	require.Error(t, err)
}

func TestSendValidation_RequestHeaders(t *testing.T) {
	var (
		accept      string
		contentType string
	)

	_, endpoint := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		accept = r.Header.Get("Accept")
		contentType = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusOK)
	})

	m := New("tok", WithEndpoint(endpoint))
	u, _ := url.Parse("https://storacha.network/validate?code=abc")
	err := m.SendValidation(context.Background(), "user@example.com", *u)
	require.NoError(t, err)
	require.Equal(t, "application/json", accept)
	require.Equal(t, "application/json", contentType)
}

func TestSendValidation_ContextCancelled(t *testing.T) {
	_, endpoint := newTestServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := New("tok", WithEndpoint(endpoint))
	u, _ := url.Parse("https://storacha.network/validate?code=abc")
	err := m.SendValidation(ctx, "user@example.com", *u)
	require.Error(t, err)
}
