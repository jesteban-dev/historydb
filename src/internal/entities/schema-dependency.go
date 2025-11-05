package entities

type DependencyType string

const (
	PSQLSequence DependencyType = "PSQLSequence"
)

// SchemaDependency is our main entity used to represent all the schema dependencies metadata in a Database.
// For relational databases -> Sequences, etc...
//
// GetDependencyType() -> Returns the dependency type
// GetName() -> Returns the schemaDependency name
// Hash() -> Returns the schemaDependency signature
// Diff() -> Returns the differences that has our schemaDependency comparing it with the parameter older schemaDependency
// ApplyDiff() -> Returns a new schemaDependency applying the differences to our schema
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type SchemaDependency interface {
	GetDependencyType() DependencyType
	GetName() string
	Hash() string
	Diff(dependency SchemaDependency) SchemaDependencyDiff
	ApplyDiff(diff SchemaDependencyDiff) SchemaDependency
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}

// SchemaDependencyDiff is an entity used to represent a reduces version of an schemaDependency that includes the
// differences it has comparing it with the previous state.
//
// Hash() -> Returns the schemaDependency signature after aplying diffs.
// GetPrevRef() -> Returns the reference to the previous entity state.
// EncodeToBytes() -> Encodes the entity into a []byte.
// DecodeFromBytes() -> Decode the entity from []byte.
type SchemaDependencyDiff interface {
	Hash() string
	GetPrevRef() string
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}
