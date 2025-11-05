package entities

// Schema is a main interface that represents the definition of a schema in a database, like a table.
type Schema interface {
	GetName() string
	Hash() ([32]byte, error)
}
