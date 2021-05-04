package table

type Store interface {
	Columns() []string
	PrimaryKey() []string
	PrimaryKeyIndices() []uint32
	GetRowHash(pkHash []byte) (rowHash []byte, ok bool)
	NumRows() int
	NewRowHashReader(offset, size int) RowHashReader
	NewRowReader() RowReader
}
