package entities

type SchemaDependency interface {
	GetName() string
	Hash() (string, error)
	Diff(dependency SchemaDependency) SchemaDependencyDiff
	ApplyDiff(diff SchemaDependencyDiff) SchemaDependency
}

type SchemaDependencyDiff interface {
	GetDependencyHash() string
}

// Schema is a main interface that represents the definition of a schema in a database, like a table.
type Schema interface {
	GetName() string
	Hash() (string, error)
	Diff(schema Schema) SchemaDiff
	ApplyDiff(diff SchemaDiff) Schema
}

type SchemaDiff interface {
	GetSchemaHash() string
}
