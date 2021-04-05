package table

type StoreType int

const (
	Small StoreType = iota
	Big
)

type RowHashReader interface {
	Read() (pkHash, rowHash []byte, err error)
	Close() error
}

type RowReader interface {
	Read() (rowHash, rowContent []byte, err error)
	Seek(offset int, whence int) (int, error)
	ReadAt(offset int) (rowHash, rowContent []byte, err error)
	Close() error
}

type Store interface {
	InsertRow(n int, pkHash, rowHash, rowContent []byte) error
	Columns() []string
	PrimaryKey() []string
	PrimaryKeyIndices() []uint32
	GetRowHash(pkHash []byte) (rowHash []byte, ok bool)
	NumRows() (int, error)
	NewRowHashReader(offset, size int) (RowHashReader, error)
	NewRowReader() (RowReader, error)
	Save() (string, error)
}
