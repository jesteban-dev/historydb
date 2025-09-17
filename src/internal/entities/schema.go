package entities

// Schema is our main entity used to represent all the schemas metadata in a Database.
// For relational databases -> Table
//
// GetName() -> Returns the schema name
// Hash() -> Returns the schema signature
// Diff() -> Returns the differences that has our schema comparing it with the parameter older schema
// ApplyDiff() -> Returns a new schema applying the differences to our schema
// EncodeToBytes() -> Encodes the entity into a []byte
type Schema interface {
	GetName() string
	Hash() string
	Diff(schema Schema) SchemaDiff
	ApplyDiff(diff SchemaDiff) Schema
	EncodeToBytes() []byte
}

// SchemaDiff is an entity used to represent a reduced version of an schema that includes the
// diferences it has comparing it with the previous state.
//
// GetSchemaHash() -> Returns the signature of the previous schema state after applying the differences
type SchemaDiff interface {
	GetSchemaHash() string
}
