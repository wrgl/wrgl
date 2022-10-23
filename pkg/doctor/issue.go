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
	Ref string `json:"ref,omitempty"`

	// Number of descendant commits
	DescendantCount int `json:"descendantCount,omitempty"`

	// Number of ancestor commits
	AncestorCount int `json:"ancestorCount,omitempty"`

	// Previous commit sum (in case commit sum is not available)
	PreviousCommit []byte `json:"previousCommit,omitempty"`

	// Commit sum
	Commit []byte `json:"commit,omitempty"`

	// Table sum if any
	Table []byte `json:"table,omitempty"`

	// Block sum if any
	Block []byte `json:"block,omitempty"`

	// Block index sum if any
	BlockIndex []byte `json:"blockIndex,omitempty"`

	// Error encountered
	Err string `json:"err,omitempty"`

	// How to resolve the issue
	Resolution Resolution `json:"resolution,omitempty"`
}
