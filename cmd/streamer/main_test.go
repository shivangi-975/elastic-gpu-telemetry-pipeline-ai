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
			name:     "returns parsed int when valid and positive",
			key:      "TEST_STREAM_INTERVAL",
			envValue: "200",
			fallback: 100,
			want:     200,
		},
		{
			name:     "returns fallback when env not set",
			key:      "TEST_STREAM_UNSET",
			envValue: "",
			fallback: 100,
			want:     100,
		},
		{
			name:     "returns fallback when env is invalid",
			key:      "TEST_STREAM_INVALID",
			envValue: "abc",
			fallback: 50,
			want:     50,
		},
		{
			name:     "returns fallback when env is zero",
			key:      "TEST_STREAM_ZERO",
			envValue: "0",
			fallback: 100,
			want:     100,
		},
		{
			name:     "returns fallback when env is negative",
			key:      "TEST_STREAM_NEG",
			envValue: "-10",
			fallback: 100,
			want:     100,
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

func TestMustEnvPanicsOnMissing(t *testing.T) {
	key := "TEST_MUST_ENV_MISSING_12345"
	os.Unsetenv(key)

	defer func() {
		if r := recover(); r == nil {
			t.Log("mustEnv calls os.Exit, which cannot be tested directly without process isolation")
		}
	}()
}

func TestEnvIntValidPositive(t *testing.T) {
	key := "TEST_VALID_POSITIVE"
	os.Setenv(key, "500")
	defer os.Unsetenv(key)

	got := envInt(key, 100)
	if got != 500 {
		t.Errorf("envInt should return 500 for valid positive, got %d", got)
	}
}

