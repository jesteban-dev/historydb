package services

// BackupFactory is the interface that defines a factory for any type of backup encoding.
type BackupFactory interface {
	CreateReader() BackupReader
	CreateWriter() BackupWriter
}
