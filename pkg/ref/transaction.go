package ref

import (
	"time"

	"github.com/google/uuid"
)

type TransactionStatus string

const (
	TSInProgress TransactionStatus = "inprogress"
	TSCommitted  TransactionStatus = "committed"
)

type Transaction struct {
	ID     uuid.UUID
	Status TransactionStatus
	Begin  time.Time
	End    time.Time
}
