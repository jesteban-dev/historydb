package entities

// Schema is an interface that represents the definition of a scheme/table in a database.
type Schema interface {
	Hash() ([32]byte, error)
	ToJSON() ([]byte, error)
}

// DBExtraInfo is an interface that represents a struct with all any other type of objects in a database that needs to be read,
// but it is not common to all database types.
type DBExtraInfo interface{}

// SchemaData is an interface that represents a single data/row inside a scheme/table.
type SchemaData interface{}

// BatchCursor is an interface that represents a Cursor to make batched queries to a database scheme.
type BatchCursor interface{}
