package table

type RowHashReader interface {
	Read() (pkHash, rowHash []byte, err error)
}

type RowReader interface {
	Read() (rowHash, rowContent []byte, err error)
}

type Store interface {
	InsertRow(n int, pkHash, rowHash, rowContent []byte) error
	Columns() []string
	PrimaryKey() []string
	GetRowHash(pkHash []byte) (rowHash []byte, ok bool)
	NumRows() int
	NewRowHashReader(offset, size int) RowHashReader
	NewRowReader(offset, size int) RowReader
	Save() (string, error)
}
