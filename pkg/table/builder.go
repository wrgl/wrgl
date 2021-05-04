package table

import (
	"bytes"
	"fmt"
	"sort"
	"sync"

	"github.com/mmcloughlin/meow"
	"github.com/wrgl/core/pkg/kv"
	"github.com/wrgl/core/pkg/misc"
	"github.com/wrgl/core/pkg/objects"
)

const (
	maxRowsGrow              = 1 << 22
	defaultBigStoreThreshold = 1 << 24
)

var tableIndexPrefix = []byte("table_indices/")

func tableIndexKey(hash []byte) []byte {
	return append(tableIndexPrefix, hash...)
}

// Builder insert rows and build table object in a thread-safe manner.
type Builder struct {
	db                kv.DB
	fs                kv.FileStore
	table             *objects.Table
	seed              uint64
	mu                sync.Mutex
	grow              int
	bigStoreThreshold int
}

func NewBuilder(db kv.DB, fs kv.FileStore, columns []string, primaryKeyIndices []uint32, seed uint64, bigStoreThreshold int) *Builder {
	if bigStoreThreshold == 0 {
		bigStoreThreshold = defaultBigStoreThreshold
	}
	return &Builder{
		db: db,
		fs: fs,
		table: &objects.Table{
			Columns: columns,
			PK:      primaryKeyIndices,
		},
		seed:              seed,
		grow:              2,
		bigStoreThreshold: bigStoreThreshold,
	}
}

func (b *Builder) growSize() int {
	b.grow = b.grow << 1
	if b.grow > maxRowsGrow {
		b.grow = maxRowsGrow
	}
	return b.grow
}

func (b *Builder) maybeGrowRows(n int) {
	l := len(b.table.Rows)
	if n < l {
		return
	}
	c := cap(b.table.Rows)
	if n > c+maxRowsGrow {
		panic(fmt.Sprintf("asking for too much space in advance: %d", n-c))
	}
	newlen := c
	for n >= newlen {
		newlen += b.growSize()
	}
	if newlen > c {
		sl := make([][]byte, n+1, newlen)
		copy(sl, b.table.Rows)
		b.table.Rows = sl
	} else {
		b.table.Rows = b.table.Rows[:n+1]
	}
}

func (b *Builder) InsertRow(n int, pkHash, rowHash, rowContent []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	kh := append(pkHash, rowHash...)
	b.maybeGrowRows(n)
	b.table.Rows[n] = kh
	return SaveRow(b.db, rowHash, rowContent)
}

func (b *Builder) saveBigTable(sum, content []byte) error {
	w, err := b.fs.Writer(tableKey(sum))
	if err != nil {
		return err
	}
	defer w.Close()
	_, err = w.Write(content)
	if err != nil {
		return err
	}
	w, err = b.fs.Writer(tableIndexKey(sum))
	if err != nil {
		return err
	}
	defer w.Close()
	iw := NewHashIndexWriter(w, b.table.Rows)
	return iw.Flush()
}

func (b *Builder) SaveTable() ([]byte, error) {
	buf := misc.NewBuffer(nil)
	n := len(b.table.Rows)
	writer := objects.NewTableWriter(buf)
	err := writer.WriteTable(b.table)
	if err != nil {
		return nil, err
	}
	v := buf.Bytes()
	sum := meow.Checksum(b.seed, v)
	if n <= b.bigStoreThreshold {
		err = b.db.Set(tableKey(sum[:]), v)
	} else {
		err = b.saveBigTable(sum[:], v)
	}
	if err != nil {
		return nil, err
	}
	return sum[:], nil
}

func ReadTable(db kv.DB, fs kv.FileStore, hash []byte) (Store, error) {
	k := tableKey(hash)
	if db.Exist(k) {
		v, err := db.Get(k)
		if err != nil {
			return nil, err
		}
		reader, err := objects.NewTableReader(bytes.NewReader(v))
		if err != nil {
			return nil, err
		}
		return &SmallStore{
			reader: reader,
			db:     db,
		}, nil
	} else if fs.Exist(k) {
		content, err := fs.Reader(k)
		if err != nil {
			return nil, err
		}
		reader, err := objects.NewTableReader(content)
		if err != nil {
			return nil, err
		}
		indexContent, err := fs.Reader(tableIndexKey(hash))
		if err != nil {
			return nil, err
		}
		index, err := NewHashIndex(indexContent)
		if err != nil {
			return nil, err
		}
		return &BigStore{
			reader: reader,
			index:  index,
			db:     db,
		}, nil
	}
	return nil, kv.KeyNotFoundError
}

func DeleteTable(db kv.DB, fs kv.FileStore, hash []byte) error {
	k := tableKey(hash)
	if db.Exist(k) {
		return db.Delete(k)
	} else if fs.Exist(k) {
		err := fs.Delete(k)
		if err != nil {
			return err
		}
		return fs.Delete(tableIndexKey(hash))
	}
	return kv.KeyNotFoundError
}

func GetAllTableHashes(db kv.DB, fs kv.FileStore) (sl [][]byte, err error) {
	sl1, err := db.FilterKey(tablePrefix)
	if err != nil {
		return nil, err
	}
	sl2, err := fs.FilterKey(tablePrefix)
	if err != nil {
		return nil, err
	}
	sl = make([][]byte, 0, len(sl1)+len(sl2))
	n := len(tablePrefix)
	for _, b := range sl1 {
		sl = append(sl, b[n:])
	}
	for _, b := range sl2 {
		sl = append(sl, b[n:])
	}
	sort.Slice(sl, func(i, j int) bool {
		return string(sl[i]) < string(sl[j])
	})
	return sl, nil
}
