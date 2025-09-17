package database_services

// DatabaseFactory is the interface that defines a factory for any type of database.
//
// CreateReader() -> Creates the Reader to query the DB.
// CreateWriter() -> Creates the Writer to insert data into the DB.
// GetDBEngine() -> Returns the DB engine.
// CheckBackupDB() -> Check the backup engine matches the DB engine.
type DatabaseFactory interface {
	CreateReader() DatabaseReader
	CreateWriter() DatabaseWriter
	GetDBEngine() string
	CheckBackupDB(engine string) bool
}
