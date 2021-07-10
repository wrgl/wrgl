package sorter

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"sort"

	"github.com/schollz/progressbar/v3"
	"github.com/wrgl/core/pkg/mem"
	"github.com/wrgl/core/pkg/objects"
	"github.com/wrgl/core/pkg/slice"
)

func getRunSize() (uint64, error) {
	total, err := mem.GetTotalMem()
	if err != nil {
		return 0, err
	}
	avail, err := mem.GetAvailMem()
	if err != nil {
		return 0, err
	}
	size := avail
	if size < total/8 {
		size = total / 8
	}
	return size / 2, nil
}

func writeChunk(rows [][]string) (*os.File, error) {
	f, err := ioutil.TempFile("", "sorted_chunk_*")
	if err != nil {
		return nil, err
	}
	enc := objects.NewStrListEncoder(true)
	for _, row := range rows {
		b := enc.Encode(row)
		_, err := f.Write(b)
		if err != nil {
			return nil, err
		}
	}
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}
	return f, nil
}

// Sorter sorts input CSV based on PK and output blocks of 255 rows each
type Sorter struct {
	PK        []uint32
	runSize   uint64
	bar       *progressbar.ProgressBar
	size      uint64
	out       io.Writer
	chunks    []*os.File
	current   [][]string
	Columns   []string
	RowsCount uint32
}

func SortRows(blk [][]string, pk []uint32) {
	sort.Slice(blk, func(i, j int) bool {
		return rowIsLess(pk, blk[i], blk[j])
	})
}

func NewSorter(runSize uint64, out io.Writer) (s *Sorter, err error) {
	if runSize == 0 {
		runSize, err = getRunSize()
		if err != nil {
			return
		}
	}
	s = &Sorter{
		out:     out,
		runSize: runSize,
	}
	return
}

func (s *Sorter) AddRow(row []string) error {
	s.size += 4
	for _, str := range row {
		s.size += uint64(len(str)) + 2
	}
	if s.bar != nil {
		s.bar.Add(1)
	}
	s.RowsCount++
	l := len(s.current)
	if l == cap(s.current) {
		s.current = append(s.current, nil)
	} else {
		s.current = s.current[:l+1]
	}
	if s.current[l] == nil {
		s.current[l] = make([]string, len(row))
	}
	copy(s.current[l], row)
	if s.size >= s.runSize {
		s.size = 0
		SortRows(s.current, s.PK)
		chunk, err := writeChunk(s.current)
		if err != nil {
			return err
		}
		s.chunks = append(s.chunks, chunk)
		s.current = s.current[:0]
	}
	return nil
}

func (s *Sorter) SortFile(f io.ReadCloser, pk []string) (err error) {
	r := csv.NewReader(f)
	r.ReuseRecord = true
	row, err := r.Read()
	if err != nil {
		return
	}
	s.Columns = make([]string, len(row))
	copy(s.Columns, row)
	s.PK, err = slice.KeyIndices(s.Columns, pk)
	if err != nil {
		return
	}
	s.bar = pbar(-1, "sorting", s.out)
	s.size = 0
	for {
		row, err = r.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return
		}
		s.AddRow(row)
	}
	err = s.bar.Finish()
	if err != nil {
		return
	}
	return f.Close()
}

type Block struct {
	Offset int
	Block  []byte
	PK     []string
}

type Rows struct {
	Offset int
	Rows   [][]string
}

func (s *Sorter) removeCols(row []string, removedCols map[int]struct{}) []string {
	if removedCols == nil {
		return row
	}
	strs := make([]string, 0, len(row)-len(removedCols))
	for i, s := range row {
		if _, ok := removedCols[i]; ok {
			continue
		}
		strs = append(strs, s)
	}
	return strs
}

func (s *Sorter) SortedBlocks(removedCols map[int]struct{}, errChan chan<- error) (blocks chan *Block) {
	blocks = make(chan *Block, 10)
	go func() {
		defer close(blocks)
		blk := make([][]byte, 0, 255)
		offset := 0
		var pk []string
		n := len(s.chunks)
		chunkRows := make([]objects.StrList, n)
		chunkEOF := make([]bool, n)
		dec := objects.NewStrListDecoder(true)
		enc := objects.NewStrListEncoder(false)
		SortRows(s.current, s.PK)
		var currentBlock objects.StrList
		r := objects.NewColumnRemover(removedCols)
		for {
			minInd := 0
			var minRow []byte
			for i, chunk := range s.chunks {
				if chunkEOF[i] {
					continue
				}
				if chunkRows[i] == nil {
					_, b, err := dec.ReadBytes(chunk)
					if err == io.EOF {
						chunkEOF[i] = true
						err = s.chunks[i].Close()
						if err != nil {
							errChan <- err
							return
						}
						continue
					}
					if err != nil {
						errChan <- err
						return
					}
					chunkRows[i] = b
				}
				if minRow == nil || chunkRows[i].LessThan(s.PK, minRow) {
					minRow = chunkRows[i]
					minInd = i
				}
			}
			if len(s.current) > 0 {
				if currentBlock == nil {
					currentBlock = enc.Encode(s.current[0])
				}
				if minRow == nil || currentBlock.LessThan(s.PK, minRow) {
					minRow = currentBlock
					minInd = n
				}
			}
			if minRow == nil {
				break
			}
			blk = append(blk, r.RemoveFrom(minRow))
			minRow = nil
			if len(blk) == 1 {
				pk = objects.StrList(blk[0]).ReadColumns(s.PK)
			}
			if minInd < n {
				chunkRows[minInd] = nil
			} else {
				s.current = s.current[1:]
				currentBlock = nil
			}
			if len(blk) == 255 {
				blocks <- &Block{
					Offset: offset,
					Block:  objects.CombineRowBytesIntoBlock(blk),
					PK:     pk,
				}
				offset++
				blk = blk[:0]
			}
		}
		if len(blk) > 0 {
			blocks <- &Block{
				Offset: offset,
				Block:  objects.CombineRowBytesIntoBlock(blk),
				PK:     pk,
			}
		}
	}()
	return
}

func rowIsLess(pk []uint32, a, b []string) bool {
	for _, u := range pk {
		if a[u] < b[u] {
			return true
		} else if a[u] > b[u] {
			return false
		}
	}
	return false
}

func (s *Sorter) SortedRows(removedCols map[int]struct{}, errChan chan<- error) (rowsCh chan *Rows) {
	rowsCh = make(chan *Rows, 10)
	go func() {
		defer close(rowsCh)
		rows := make([][]string, 0, 255)
		offset := 0
		n := len(s.chunks)
		chunkRows := make([][]string, n)
		chunkEOF := make([]bool, n)
		dec := objects.NewStrListDecoder(false)
		SortRows(s.current, s.PK)
		for {
			minInd := 0
			var minRow []string
			for i, chunk := range s.chunks {
				if chunkEOF[i] {
					continue
				}
				if chunkRows[i] == nil {
					_, row, err := dec.Read(chunk)
					if err == io.EOF {
						chunkEOF[i] = true
						err = s.chunks[i].Close()
						if err != nil {
							errChan <- err
							return
						}
						continue
					}
					if err != nil {
						errChan <- err
						return
					}
					chunkRows[i] = row
				}
				if minRow == nil {
					minRow = chunkRows[i]
					minInd = i
				} else {
					for _, u := range s.PK {
						if chunkRows[i][u] < minRow[u] {
							minRow = chunkRows[i]
							minInd = i
							break
						}
					}
				}
			}
			if len(s.current) > 0 {
				if minRow == nil {
					minRow = s.current[0]
					minInd = n
				} else {
					for _, u := range s.PK {
						if s.current[0][u] < minRow[u] {
							minRow = s.current[0]
							minInd = n
							break
						}
					}
				}
			}
			if minRow == nil {
				break
			}
			rows = append(rows, s.removeCols(minRow, removedCols))
			if minInd < n {
				chunkRows[minInd] = nil
			} else {
				s.current = s.current[1:]
			}
			if len(rows) == 255 {
				rowsCh <- &Rows{
					Offset: offset,
					Rows:   rows,
				}
				offset++
				rows = make([][]string, 0, 255)
			}
		}
		if len(rows) > 0 {
			rowsCh <- &Rows{
				Offset: offset,
				Rows:   rows,
			}
		}
	}()
	return rowsCh
}

func (s *Sorter) Close() error {
	for _, f := range s.chunks {
		f.Close()
		if err := os.Remove(f.Name()); err != nil {
			return err
		}
	}
	return nil
}
