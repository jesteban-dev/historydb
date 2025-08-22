package entities

import "time"

type Snapshot struct {
	Id        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Schemas   map[string]string `json:"schemas"`
}
