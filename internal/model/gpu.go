package model

import "time"

// GPU represents a GPU device in the system.
//
// UUID is the unique identifier for the GPU.
// Hostname is the name of the host where the GPU is located.
// ModelName is the human-readable model string sourced from DCGM
// (e.g. "NVIDIA H100 80GB HBM3"). May be empty for older records.
// LastSeen is the last time the GPU was observed as active.
//
// @Description GPU device record
type GPU struct {
	UUID      string    `json:"uuid"`
	Hostname  string    `json:"hostname"`
	ModelName string    `json:"model_name"`
	LastSeen  time.Time `json:"last_seen"`
}
