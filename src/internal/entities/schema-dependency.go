package entities

type DependencyType string

const (
	PSQLSequence DependencyType = "PSQLSequence"
)

// SchemaDependency is our main entity used to represent all the schema dependencies metadata in a Database.
// For relational databases -> Sequences, etc...
//
// GetName() -> Returns the schemaDependency name
// Hash() -> Returns the schemaDependency signature
// Diff() -> Returns the differences that has our schemaDependency comparing it with the parameter older schemaDependency
// ApplyDiff() -> Returns a new schemaDependency applying the differences to our schema
// EncodeToBytes() -> Encodes the entity into a []byte
type SchemaDependency interface {
	GetName() string
	Hash() string
	Diff(dependency SchemaDependency) SchemaDependencyDiff
	ApplyDiff(diff SchemaDependencyDiff) SchemaDependency
	EncodeToBytes() []byte
}

// SchemaDependencyDiff is an entity used to represent a reduces version of an schemaDependency that includes the
// differences it has comparing it with the previous state.
//
// GetSchemaDependencyHash() -> Returns the signature of the previous schemDependency state after applying the differences
// EncodeToBytes() -> Encodes the entity into a []byte
type SchemaDependencyDiff interface {
	GetSchemaDependencyHash() string
	EncodeToBytes() []byte
}
