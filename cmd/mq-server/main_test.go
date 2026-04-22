package main

import (
	"os"
	"testing"
)

func TestEnvInt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		key      string
		envValue string
		fallback int
		want     int
	}{
		{
			name:     "returns parsed int when valid",
			key:      "TEST_MQ_PARTITIONS",
			envValue: "16",
			fallback: 8,
			want:     16,
		},
		{
			name:     "returns fallback when env not set",
			key:      "TEST_MQ_UNSET",
			envValue: "",
			fallback: 8,
			want:     8,
		},
		{
			name:     "returns fallback when env is invalid",
			key:      "TEST_MQ_INVALID",
			envValue: "not-a-number",
			fallback: 4096,
			want:     4096,
		},
		{
			name:     "returns zero when env is zero",
			key:      "TEST_MQ_ZERO",
			envValue: "0",
			fallback: 100,
			want:     0,
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

			got := envInt(tc.key, tc.fallback)
			if got != tc.want {
				t.Errorf("envInt(%q, %d) = %d; want %d", tc.key, tc.fallback, got, tc.want)
			}
		})
	}
}

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
			key:      "TEST_MQ_ADDR",
			envValue: ":8080",
			fallback: ":9000",
			want:     ":8080",
		},
		{
			name:     "returns fallback when env not set",
			key:      "TEST_MQ_ADDR_UNSET",
			envValue: "",
			fallback: ":9000",
			want:     ":9000",
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

func TestEnvIntNegativeValue(t *testing.T) {
	key := "TEST_MQ_NEGATIVE"
	os.Setenv(key, "-5")
	defer os.Unsetenv(key)

	got := envInt(key, 8)
	if got != -5 {
		t.Errorf("envInt with negative value should return -5, got %d", got)
	}
}

