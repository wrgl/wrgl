package diff

import (
	"bytes"
	"encoding/hex"
	"testing"

	"github.com/mmcloughlin/meow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/objects"
	objmock "github.com/wrgl/wrgl/pkg/objects/mock"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func saveBlock(t *testing.T, db objects.Store, blk [][]string, pk []uint32) (sum []byte, idxSum []byte, idx *objects.BlockIndex) {
	t.Helper()
	buf := bytes.NewBuffer(nil)
	enc := objects.NewStrListEncoder(true)
	_, err := objects.WriteBlockTo(enc, buf, blk)
	require.NoError(t, err)
	sum, err = objects.SaveBlock(db, buf.Bytes())
	require.NoError(t, err)
	hash := meow.New(0)
	idx, err = objects.IndexBlock(enc, hash, blk, pk)
	require.NoError(t, err)
	buf.Reset()
	_, err = idx.WriteTo(buf)
	require.NoError(t, err)
	idxSum, err = objects.SaveBlockIndex(db, buf.Bytes())
	require.NoError(t, err)
	return sum, idxSum, idx
}

func createTableFromBlock(t *testing.T, db objects.Store, columns []string, pk []uint32, blks [][][]string) (*objects.Table, [][]string) {
	tbl := &objects.Table{
		Columns: columns,
		PK:      pk,
	}
	tblIdx := make([][]string, len(blks))
	for i, blk := range blks {
		sum, idxSum, _ := saveBlock(t, db, blk, pk)
		tbl.Blocks = append(tbl.Blocks, sum)
		tbl.BlockIndices = append(tbl.BlockIndices, idxSum)
		tbl.RowsCount += uint32(len(blk))
		tblIdx[i] = slice.IndicesToValues(blk[0], pk)
	}
	return tbl, tblIdx
}

func TestFindOverlappingBlocks(t *testing.T) {
	for i, c := range []struct {
		idx1, idx2               [][]string
		off, prevEnd, start, end int
	}{
		{
			[][]string{{"1"}},
			[][]string{{"2"}, {"3"}},
			0, 0, 0, 2,
		},
		{
			[][]string{{"1"}},
			[][]string{{"1"}, {"2"}},
			0, 0, 0, 2,
		},
		{
			[][]string{{"2"}},
			[][]string{{"1"}, {"3"}},
			0, 0, 0, 2,
		},
		{
			[][]string{{"2"}},
			[][]string{{"1"}, {"2"}},
			0, 0, 1, 2,
		},
		{
			[][]string{{"3"}},
			[][]string{{"1"}, {"2"}},
			0, 0, 1, 2,
		},
		{
			[][]string{{"1"}, {"2"}},
			[][]string{{"2"}, {"3"}},
			0, 0, 0, 0,
		},
		{
			[][]string{{"1"}, {"2"}},
			[][]string{{"1"}, {"2"}},
			0, 0, 0, 1,
		},
		{
			[][]string{{"2"}, {"4"}},
			[][]string{{"1"}, {"3"}, {"5"}},
			0, 0, 0, 2,
		},
		{
			[][]string{{"2"}, {"3"}},
			[][]string{{"1"}, {"2"}, {"3"}},
			0, 0, 1, 2,
		},
		{
			[][]string{{"3"}, {"5"}},
			[][]string{{"1"}, {"2"}, {"4"}},
			0, 0, 1, 3,
		},
		{
			[][]string{{"3"}, {"5"}},
			[][]string{{"1"}, {"2"}, {"3"}},
			0, 0, 2, 3,
		},
	} {
		start, end := findOverlappingBlocks(c.idx1, c.idx2, c.off, c.prevEnd)
		assert.Equal(t, c.start, start, "start in case %d", i)
		assert.Equal(t, c.end, end, "end in case %d", i)
	}
}

func TestGetBlockIndices(t *testing.T) {
	db := objmock.NewStore()
	indices := []*objects.BlockIndex{}
	tbl := &objects.Table{}
	for i := 0; i < 10; i++ {
		blk := testutils.BuildRawCSV(4, 255)[1:]
		sum, idxSum, idx := saveBlock(t, db, blk, []uint32{0})
		tbl.Blocks = append(tbl.Blocks, sum)
		tbl.BlockIndices = append(tbl.BlockIndices, idxSum)
		indices = append(indices, idx)
	}

	for i, c := range []struct {
		prevStart, prevEnd int
		prevSl             []*objects.BlockIndex
		start, end         int
		sl                 []*objects.BlockIndex
	}{
		{
			0, 0, nil,
			0, 2, indices[:2],
		},
		{
			0, 2, indices[:2],
			0, 2, indices[:2],
		},
		{
			0, 2, indices[:2],
			0, 3, indices[:3],
		},
		{
			0, 2, indices[:2],
			1, 3, indices[1:3],
		},
		{
			0, 2, indices[:2],
			2, 4, indices[2:4],
		},
		{
			0, 2, indices[:2],
			3, 5, indices[3:5],
		},
	} {
		sl, err := getBlockIndices(db, tbl, c.start, c.end, c.prevSl, c.prevStart, c.prevEnd)
		require.NoError(t, err)
		assert.Equal(t, c.sl, sl, "case %d", i)
	}
}

func hexToBytes(t *testing.T, s string) []byte {
	t.Helper()
	b, err := hex.DecodeString(s)
	require.NoError(t, err)
	return b
}

func TestIterateAndMatch(t *testing.T) {
	db := objmock.NewStore()
	tbl1, tblIdx1 := createTableFromBlock(t, db, []string{"a", "b", "c"}, []uint32{0}, [][][]string{
		{
			{"01", "q", "w"},
			{"02", "a", "s"},
		},
		{
			{"05", "z", "x"},
			{"06", "e", "r"},
			{"07", "d", "f"},
			{"08", "c", "v"},
		},
		{
			{"11", "t", "y"},
			{"12", "g", "h"},
		},
	})
	tbl2, tblIdx2 := createTableFromBlock(t, db, []string{"a", "b", "c"}, []uint32{0}, [][][]string{
		{
			{"03", "b", "n"},
			{"04", "u", "i"},
		},
		{
			{"05", "q", "w"},
			{"07", "a", "s"},
		},
		{
			{"09", "j", "k"},
			{"10", "o", "p"},
		},
	})
	type row struct {
		pk, row1, row2 []byte
		off1, off2     uint32
	}
	rows := []*row{}
	err := iterateAndMatch(db, db, tbl1, tbl2, tblIdx1, tblIdx2, nil, func(pk, row1, row2 []byte, off1, off2 uint32) {
		rows = append(rows, &row{
			pk, row1, row2, off1, off2,
		})
	})
	require.NoError(t, err)
	assert.Equal(t, []*row{
		{
			pk:   hexToBytes(t, "2ac3b809e14699aa43167930405d0249"),
			row1: hexToBytes(t, "eb88cfa1940c985495ee43bd80678b59"),
		},
		{
			pk:   hexToBytes(t, "5c8a428becc36c58e262b7a7b6f262cb"),
			row1: hexToBytes(t, "ab9f4d3e508b7e889233e31b7565e335"),
			off1: 1,
		},
		{
			pk:   hexToBytes(t, "6fec011086808a2717703219894d2092"),
			row1: hexToBytes(t, "2dd2f57a2b7a4f3c9d40b3beb680bf70"),
			row2: hexToBytes(t, "2d6792228e34229f729fa524f64b7035"),
			off1: 255,
			off2: 255,
		},
		{
			pk:   hexToBytes(t, "e10d9c0c56fd44d2a840b35c59de5146"),
			row1: hexToBytes(t, "56c67649f8c5962465c0e4c1f0f51d34"),
			off1: 256,
		},
		{
			pk:   hexToBytes(t, "89fabb8585b03215264fe5482367a9f4"),
			row1: hexToBytes(t, "bd85083827c141fbcfbbf9e2a26186d3"),
			row2: hexToBytes(t, "4aaeb7a6ba9e9b6ac650d8f7bddab8e3"),
			off1: 257,
			off2: 256,
		},
		{
			pk:   hexToBytes(t, "13cb49e3ae9d636e1540cbfe919c27aa"),
			row1: hexToBytes(t, "084a3c6660b3fc92933933619d760698"),
			off1: 258,
		},
		{
			pk:   hexToBytes(t, "1ac42db50c18a6838392053c781da0e9"),
			row1: hexToBytes(t, "66f0fefd52586006827ac03bff10dda4"),
			off1: 510,
		},
		{
			pk:   hexToBytes(t, "28ff73b200861617c8eb0947f7a44f89"),
			row1: hexToBytes(t, "5cb09a378634818b4489250edf1714ee"),
			off1: 511,
		},
	}, rows)
}
