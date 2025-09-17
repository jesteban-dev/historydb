package entities

import "time"

// BackupMetadata defines the main metadata file used in the backups
//
// DatabaseEngine -> The DB Engine used in the backup
// Snapshots -> List of all snapshots taken in the backup
type BackupMetadata struct {
	DatabaseEngine string           `json:"dbEngine"`
	Snapshots      []BackupSnapshot `json:"snapshots"`
}

// BackupSnapshot defines the main info for all the snapshots taken in the backup
//
// Id -> Snaphost Id
// Timestamp -> Timestamp the snapshot was taken
// Schemas -> map that links every schema with its schema backup file
// Data -> map that link every schema with its schema backup data files
type BackupSnapshot struct {
	Id                 string                              `json:"id"`
	Timestamp          time.Time                           `json:"timestamp"`
	SchemaDependencies map[string]string                   `json:"schemaDependencies"`
	Schemas            map[string]string                   `json:"schemas"`
	Data               map[string]BackupSnapshotSchemaData `json:"data"`
}

// BackupSnapshotSchemaData defines the info saved from each schema in a snapshot
// that serves to rebuild the schema data.
//
// BatchSize -> The max-size for all batches used to save the schema data.
// ChunkSize -> The max-size for all chunks used to save the schema data.
// Data -> A string of paths that represents all the batch files needed to rebuild the schema data.
type BackupSnapshotSchemaData struct {
	BatchSize int      `json:"batchSize"`
	ChunkSize int      `json:"chunkSize"`
	Data      []string `json:"data"`
}
