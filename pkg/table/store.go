package table

type RowHashReader interface {
	Read() (pkHash, rowHash []byte, err error)
	Close() error
}

type RowReader interface {
	Read() (rowHash, rowContent []byte, err error)
	Close() error
}

type Store interface {
	InsertRow(n int, pkHash, rowHash, rowContent []byte) error
	Columns() []string
	PrimaryKey() []string
	GetRowHash(pkHash []byte) (rowHash []byte, ok bool)
	NumRows() (int, error)
	NewRowHashReader(offset, size int) (RowHashReader, error)
	NewRowReader(offset, size int) (RowReader, error)
	Save() (string, error)
}
