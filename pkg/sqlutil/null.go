package sqlutil

import (
	"database/sql/driver"
	"fmt"
)

type NullBlob struct {
	Blob  []byte
	Valid bool // Valid is true if String is not NULL
}

// Scan implements the Scanner interface.
func (ns *NullBlob) Scan(value interface{}) error {
	if value == nil {
		ns.Blob, ns.Valid = nil, false
		return nil
	}
	ns.Valid = true
	switch v := value.(type) {
	case string:
		ns.Blob = []byte(v)
	case []byte:
		ns.Blob = make([]byte, len(v))
		copy(ns.Blob, v)
	default:
		return fmt.Errorf("unhandled type %T", v)
	}
	return nil
}

// Value implements the driver Valuer interface.
func (ns NullBlob) Value() (driver.Value, error) {
	if !ns.Valid {
		return nil, nil
	}
	return ns.Blob, nil
}
