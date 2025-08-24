package entities

// Schema is a main interface that represents the definition of a schema in a database, like a table.
type Schema interface {
	GetName() string
	Hash() (string, error)
	Diff(schema Schema) SchemaDiff
	ApplyDiff(diff SchemaDiff) Schema
}

type SchemaDiff interface {
	GetPrevRef() string
	GetSchemaHash() string
}
