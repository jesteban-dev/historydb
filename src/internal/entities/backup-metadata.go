package entities

import "time"

type BackupMetadata struct {
	Database  BackupDatabase   `json:"database"`
	Snapshots []BackupSnapshot `json:"snapshots"`
}

type BackupDatabase struct {
	Engine string `json:"engine"`
	Host   string `json:"host"`
	Port   int    `json:"port"`
	DbName string `json:"dbName"`
}

type BackupSnapshot struct {
	Id        string            `json:"id"`
	Timestamp time.Time         `json:"timestamp"`
	Schemas   map[string]string `json:"schemas"`
}
