package model

import "time"

// GPU represents a GPU device in the system.
//
// UUID is the unique identifier for the GPU.
// Hostname is the name of the host where the GPU is located.
// LastSeen is the last time the GPU was observed as active.
//
// @Description GPU device record
type GPU struct {
	UUID     string    `json:"uuid"`
	Hostname string    `json:"hostname"`
	LastSeen time.Time `json:"last_seen"`
}
