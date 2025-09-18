package backup_services

// BackupFactory is the interface that defines a factory for any type of backup encoding.
//
// CreateReader() -> Creates the Reader to retrive data from the Backup.
// CreateWriter() -> Creates the Writer to save data into the Backup.
// GetBackupEncoding() -> Returns the encoding used in the Backup.
type BackupFactory interface {
	CreateReader() BackupReader
	CreateWriter() BackupWriter
	GetBackupEncoding() string
}
