package entities

type RoutineType string

const (
	PSQLFunction  RoutineType = "PSQLFunction"
	PSQLProcedure RoutineType = "PSQLProcedure"
	PSQLTrigger   RoutineType = "PSQLTrigger"
)

type Routine interface {
	GetName() string
	GetRoutineType() RoutineType
	Hash() string
	Diff(routine Routine, isDiff bool) RoutineDiff
	ApplyDiff(diff RoutineDiff) Routine
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}

type RoutineDiff interface {
	Hash() string
	GetPrevRef() string
	EncodeToBytes() []byte
	DecodeFromBytes(data []byte) error
}
