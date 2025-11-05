package entities

type SchemaType string

const (
	SQLTable SchemaType = "SQLTable"
)

// Schema is our main entity used to represent all the schemas metadata in a Database.
//
// GetSchemaType() -> Returns the schema type
// GetName() -> Returns the schema name
// Hash() -> Returns the schema signature
// Diff() -> Returns the differences that has our schema comparing it with the parameter older schema
// ApplyDiff() -> Returns a new schema applying the differences to our schema
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type Schema interface {
	GetSchemaType() SchemaType
	GetName() string
	Hash() string
	Diff(schema Schema, isDiff bool) SchemaDiff
	ApplyDiff(diff SchemaDiff) Schema
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}

// SchemaDiff is an entity used to represent a reduced version of an schema that includes the
// differences it has comparing it with the previous state.
//
// Hash() -> Returns the schema signature after applying diffs
// GetPrevRef() -> Returns the reference to the previous entity state
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type SchemaDiff interface {
	Hash() string
	GetPrevRef() string
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}
