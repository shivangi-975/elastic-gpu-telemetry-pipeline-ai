package mq

import "hash/fnv"

// PartitionRouter maps message keys to partition indices using fnv32a hashing.
type PartitionRouter struct {
	NumPartitions int
}

// Route returns the partition index for the given key.
// The same key always maps to the same partition.
func (r *PartitionRouter) Route(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % r.NumPartitions
}
