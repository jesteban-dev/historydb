package services

// BackupFactory is the interface that defines a factory fot any type of backup encoding.
type BackupFactory interface {
	CreateWriter() BackupWriter
}
