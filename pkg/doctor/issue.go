package doctor

type Resolution string

const (
	UnknownResolution  Resolution = "unknown"
	ResetPKResolution  Resolution = "resetPK"
	ReingestResolution Resolution = "reingest"
	RemoveResolution   Resolution = "remove"
)

type Issue struct {
	// Name of wrgl reference
	Ref string

	// Number of descendant commits
	DescendantCount int

	// Number of ancestor commits
	AncestorCount int

	// Previous commit sum (in case commit sum is not available)
	PreviousCommit []byte

	// Commit sum
	Commit []byte

	// Table sum if any
	Table []byte

	// Block sum if any
	Block []byte

	// Block index sum if any
	BlockIndex []byte

	// Error encountered
	Err string

	// How to resolve the issue
	Resolution Resolution
}
