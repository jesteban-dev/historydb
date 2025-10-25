package entities

type RoutineType string

const (
	PSQLFunction  RoutineType = "PSQLFunction"
	PSQLProcedure RoutineType = "PSQLProcedure"
	PSQLTrigger   RoutineType = "PSQLTrigger"
)

// Routine is our main entity used to represent all the routines metadata in a Database.
//
// GetName() -> Returns the routine name
// GetRoutineType() -> Returns the routine type
// GetDependencies() -> Returns a list of others routines which it depends on
// Hash() -> Returns the routine signature
// Diff() -> Returns the differences that has our routine comparing it with parameter older routine
// ApplyDiff() -> Returns a new routine applying the differences to our routine
// EncodeToBytes() -> Encodes the entity into a []byte
// DecodeFromBytes() -> Decode the entity from []byte
type Routine interface {
	GetName() string
	GetRoutineType() RoutineType
	GetDependencies() []string
	Hash() string
	Diff(routine Routine, isDiff bool) RoutineDiff
	ApplyDiff(diff RoutineDiff) Routine
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}

// RoutineDiff is an entity used to represent a reduced version of a routine that includes the
// differences it has comparing it with the previous state.
//
// Hash() -> Returns the routine signature after applying diffs
// GetPrevRef() -> Returns the references to the previous entity state
// EncodeToBytes() -> Encodes the entity into []byte
// DecodeFromBytes() -> Decode the entity from []byte
type RoutineDiff interface {
	Hash() string
	GetPrevRef() string
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}
