package entities

import "time"

// BackupMetadata defines the main metadata file used in the backups
type BackupMetadata struct {
	Database  BackupDatabase   `json:"database"`
	Snapshots []BackupSnapshot `json:"snapshots"`
}

// BackupDatabase defines the database info that will be saved in the metadata file
// so when the user snapshots a DB, check that the DB is the correct one
type BackupDatabase struct {
	Engine string `json:"engine"`
}

// BackupSnapshot defines the main info for all the snapshots taken in the backup
// It saves an ID for the snapshot, the timestamp it was made, and a map that relationates
// the schemas with the last schema file saved.
type BackupSnapshot struct {
	Id                 string              `json:"id"`
	Timestamp          time.Time           `json:"timestamp"`
	SchemaDependencies map[string]string   `json:"schemaDependencies"`
	Schemas            map[string]string   `json:"schemas"`
	Data               map[string][]string `json:"data"`
}
