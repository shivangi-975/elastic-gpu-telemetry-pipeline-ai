package main

import (
	"os"
	"testing"
)

func TestEnvStr(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		envValue string
		fallback string
		want     string
	}{
		{
			name:     "returns env value when set",
			key:      "TEST_API_GW_VAR",
			envValue: "custom-value",
			fallback: "default",
			want:     "custom-value",
		},
		{
			name:     "returns fallback when env not set",
			key:      "TEST_API_GW_UNSET",
			envValue: "",
			fallback: "default-port",
			want:     "default-port",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if tc.envValue != "" {
				os.Setenv(tc.key, tc.envValue)
				defer os.Unsetenv(tc.key)
			} else {
				os.Unsetenv(tc.key)
			}

			got := envStr(tc.key, tc.fallback)
			if got != tc.want {
				t.Errorf("envStr(%q, %q) = %q; want %q", tc.key, tc.fallback, got, tc.want)
			}
		})
	}
}

func TestEnvStrEmptyValue(t *testing.T) {
	key := "TEST_API_GW_EMPTY"
	os.Setenv(key, "")
	defer os.Unsetenv(key)

	got := envStr(key, "fallback")
	if got != "fallback" {
		t.Errorf("envStr with empty value should return fallback, got %q", got)
	}
}

