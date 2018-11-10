package config

import "time"

type (
	// Config holds the database configuration
	Config struct {
		Storage Storage
	}

	// Storage holds the storage engine configuration
	Storage struct {
		Compaction   time.Duration
		Directory    string
		SegmentBytes int
	}
)
