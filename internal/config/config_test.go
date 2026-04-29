package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/spf13/cobra"
)

const telemetryYAML = `
telemetry:
  traces:
    endpoint: "yaml-endpoint:4318"
    sample_ratio: 0.25
  metrics:
    endpoint: "yaml-metrics:4318"
`

func writeConfigFile(t *testing.T, body string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatalf("write config: %v", err)
	}
	return path
}

// clearSprueEnv removes any SPRUE_* vars the test environment might already
// have set, so a test only sees what it deliberately exports.
func clearSprueEnv(t *testing.T) {
	t.Helper()
	for _, kv := range os.Environ() {
		for i := 0; i < len(kv); i++ {
			if kv[i] == '=' {
				name := kv[:i]
				if len(name) >= 6 && name[:6] == "SPRUE_" {
					t.Setenv(name, "")
					os.Unsetenv(name)
				}
				break
			}
		}
	}
}

// loadWithFlags re-runs the Load → BindTelemetryFlags → Unmarshal pipeline
// exactly the way cmd/main.go does it.
func loadWithFlags(t *testing.T, cfgPath string, argv []string) *Config {
	t.Helper()

	cfg, v, err := LoadWithViper(cfgPath)
	if err != nil {
		t.Fatalf("LoadWithViper: %v", err)
	}

	cmd := &cobra.Command{Use: "serve", RunE: func(*cobra.Command, []string) error { return nil }}
	RegisterTelemetryFlags(cmd)
	cmd.SetArgs(argv)
	if err := cmd.ParseFlags(argv); err != nil {
		t.Fatalf("parse flags: %v", err)
	}
	if err := BindTelemetryFlags(v, cmd); err != nil {
		t.Fatalf("bind flags: %v", err)
	}
	cfg, err = Unmarshal(v)
	if err != nil {
		t.Fatalf("re-unmarshal: %v", err)
	}
	return cfg
}

func TestTelemetry_YAMLOnly(t *testing.T) {
	clearSprueEnv(t)
	cfg := loadWithFlags(t, writeConfigFile(t, telemetryYAML), nil)

	if got := cfg.Telemetry.Traces.Endpoint; got != "yaml-endpoint:4318" {
		t.Errorf("traces endpoint = %q, want yaml-endpoint:4318", got)
	}
	if got := cfg.Telemetry.Traces.SampleRatio; got != 0.25 {
		t.Errorf("sample_ratio = %v, want 0.25", got)
	}
	if got := cfg.Telemetry.Metrics.Endpoint; got != "yaml-metrics:4318" {
		t.Errorf("metrics endpoint = %q, want yaml-metrics:4318", got)
	}
}

func TestTelemetry_EnvOverridesYAML(t *testing.T) {
	clearSprueEnv(t)
	t.Setenv("SPRUE_TELEMETRY_TRACES_ENDPOINT", "env-endpoint:4318")

	cfg := loadWithFlags(t, writeConfigFile(t, telemetryYAML), nil)

	if got := cfg.Telemetry.Traces.Endpoint; got != "env-endpoint:4318" {
		t.Errorf("traces endpoint = %q, want env-endpoint:4318 (env should win)", got)
	}
	// yaml still supplies values for fields the env did not touch.
	if got := cfg.Telemetry.Metrics.Endpoint; got != "yaml-metrics:4318" {
		t.Errorf("metrics endpoint = %q, want yaml-metrics:4318", got)
	}
}

func TestTelemetry_FlagOverridesEnv(t *testing.T) {
	clearSprueEnv(t)
	t.Setenv("SPRUE_TELEMETRY_TRACES_ENDPOINT", "env-endpoint:4318")

	cfg := loadWithFlags(t, writeConfigFile(t, telemetryYAML), []string{
		"--telemetry-traces-endpoint=flag-endpoint:4318",
	})

	if got := cfg.Telemetry.Traces.Endpoint; got != "flag-endpoint:4318" {
		t.Errorf("traces endpoint = %q, want flag-endpoint:4318 (flag should win)", got)
	}
}

// Guards the "zero-default pflag does not clobber env" regression: bind a
// flag that was never passed on the command line, and verify the env value
// still wins.
func TestTelemetry_UnsetFlagDoesNotClobberEnv(t *testing.T) {
	clearSprueEnv(t)
	t.Setenv("SPRUE_TELEMETRY_TRACES_ENDPOINT", "env-endpoint:4318")

	cfg := loadWithFlags(t, writeConfigFile(t, ""), nil)

	if got := cfg.Telemetry.Traces.Endpoint; got != "env-endpoint:4318" {
		t.Errorf("traces endpoint = %q, want env-endpoint:4318 (unset flag must not clobber env)", got)
	}
}

func TestTelemetry_HeadersFromEnv(t *testing.T) {
	clearSprueEnv(t)
	t.Setenv("SPRUE_TELEMETRY_TRACES_ENDPOINT", "env-endpoint:4318")
	t.Setenv("SPRUE_TELEMETRY_TRACES_HEADERS", "Authorization=Bearer xyz,X-Tenant=acme")

	cfg := loadWithFlags(t, writeConfigFile(t, ""), nil)

	h := cfg.Telemetry.Traces.Headers
	if got := h["Authorization"]; got != "Bearer xyz" {
		t.Errorf("Authorization header = %q, want %q", got, "Bearer xyz")
	}
	if got := h["X-Tenant"]; got != "acme" {
		t.Errorf("X-Tenant header = %q, want acme", got)
	}
}

// The composed decode hook must still decode time.Duration; this test
// catches the regression where adding a hook accidentally drops the default
// StringToTimeDurationHookFunc.
func TestTelemetry_DurationParsesAfterHookCompose(t *testing.T) {
	clearSprueEnv(t)
	t.Setenv("SPRUE_TELEMETRY_METRICS_ENDPOINT", "env-metrics:4318")
	t.Setenv("SPRUE_TELEMETRY_METRICS_EXPORT_INTERVAL", "15s")

	cfg := loadWithFlags(t, writeConfigFile(t, ""), nil)

	if got := cfg.Telemetry.Metrics.ExportInterval; got != 15*time.Second {
		t.Errorf("export_interval = %v, want 15s", got)
	}
}
