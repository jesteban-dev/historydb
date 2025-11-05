package services

// DatabaseFactory is the interface that defines a factory for any type of database.
type DatabaseFactory interface {
	CreateReader() DatabaseReader
}
