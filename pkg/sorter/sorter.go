// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package sorter

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"os"
	"sort"

	"github.com/wrgl/wrgl/pkg/dprof"
	"github.com/wrgl/wrgl/pkg/mem"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/slice"
	"github.com/wrgl/wrgl/pkg/testutils"
)

type ProgressBar interface {
	Add(int) error
	Finish() error
	Reset()
}

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
	return size / 4, nil
}

func writeChunk(rows [][]string) (*os.File, error) {
	f, err := testutils.TempFile("", "sorted_chunk_*")
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
	size      uint64
	pt        ProgressBar
	chunks    []io.Reader
	profiler  *dprof.Profiler
	current   [][]string
	Columns   []string
	cleanups  []func() error
	delimiter rune
}

type SorterOption func(s *Sorter)

func WithDelimiter(delimiter rune) SorterOption {
	return func(s *Sorter) {
		s.delimiter = delimiter
	}
}

func WithRunSize(runSize uint64) SorterOption {
	return func(s *Sorter) {
		s.runSize = runSize
	}
}

func WithProgressBar(pt ProgressBar) SorterOption {
	return func(s *Sorter) {
		s.pt = pt
	}
}

func SortRows(blk [][]string, pk []uint32) {
	sort.Slice(blk, func(i, j int) bool {
		return objects.StringSliceIsLess(pk, blk[i], blk[j])
	})
}

func NewSorter(opts ...SorterOption) (s *Sorter, err error) {
	s = &Sorter{}
	for _, opt := range opts {
		opt(s)
	}
	if s.runSize == 0 {
		s.runSize, err = getRunSize()
		if err != nil {
			return
		}
	}
	return
}

func (s *Sorter) Reset() {
	if s.current != nil {
		s.current = s.current[:0]
	}
	if s.Columns != nil {
		s.Columns = s.Columns[:0]
	}
	s.size = 0
	if s.pt != nil {
		s.pt.Reset()
	}
	if s.chunks != nil {
		s.chunks = s.chunks[:0]
	}
	if s.cleanups != nil {
		s.cleanups = s.cleanups[:0]
	}
}

func (s *Sorter) AddRow(row []string) error {
	s.size += 4
	for _, str := range row {
		s.size += uint64(len(str)) + 2
	}
	if s.pt != nil {
		s.pt.Add(1)
	}
	l := len(s.current)
	if l == cap(s.current) {
		s.current = append(s.current, nil)
	} else {
		s.current = s.current[:l+1]
	}
	if s.current[l] == nil || cap(s.current[l]) < len(row) {
		s.current[l] = make([]string, len(row))
	} else {
		s.current[l] = s.current[l][:len(row)]
	}
	copy(s.current[l], row)
	if s.size >= s.runSize {
		s.size = 0
		SortRows(s.current, s.PK)
		chunk, err := writeChunk(s.current)
		if err != nil {
			return err
		}
		s.chunks = append(s.chunks, bufio.NewReader(chunk))
		s.current = s.current[:0]
		s.cleanups = append(s.cleanups, func() error {
			err := chunk.Close()
			if err != nil {
				return err
			}
			return os.Remove(chunk.Name())
		})
	}
	return nil
}

func (s *Sorter) SetColumns(row []string) {
	s.Columns = append(s.Columns, row...)
	s.profiler = dprof.NewProfiler(s.Columns)
}

func (s *Sorter) SortFile(f io.ReadCloser, pk []string) (err error) {
	r := csv.NewReader(f)
	if s.delimiter != 0 {
		r.Comma = s.delimiter
	}
	r.ReuseRecord = true
	row, err := r.Read()
	if err != nil {
		return
	}
	s.SetColumns(row)
	s.PK, err = slice.KeyIndices(s.Columns, pk)
	if err != nil {
		return
	}
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
	if s.pt != nil {
		if err := s.pt.Finish(); err != nil {
			return err
		}
	}
	return f.Close()
}

func (s *Sorter) TableSummary() *objects.TableProfile {
	if s.profiler == nil {
		return nil
	}
	return s.profiler.Summarize()
}

type Block struct {
	Offset    int
	Block     []byte
	PK        []string
	RowsCount int
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

func (s *Sorter) pkIndices() []uint32 {
	if len(s.PK) > 0 {
		return s.PK
	}
	sl := make([]uint32, len(s.Columns))
	for i := range sl {
		sl[i] = uint32(i)
	}
	return sl
}

func (s *Sorter) SortedBlocks(ctx context.Context, removedCols map[int]struct{}, errChan chan<- error) (blocks chan *Block) {
	blocks = make(chan *Block, 10)
	pkIndices := s.pkIndices()
	go func() {
		defer close(blocks)
		blk := make([][]byte, 0, 255)
		offset := 0
		blkPK := make([]string, 0, len(pkIndices))
		rowPK := make([]string, len(pkIndices))
		prevRowPK := make([]string, len(pkIndices))
		dec := objects.NewStrListDecoder(true)
		n := len(s.chunks)
		chunkRows := make([]objects.StrList, n)
		chunkEOF := make([]bool, n)
		enc := objects.NewStrListEncoder(false)
		SortRows(s.current, s.PK)
		var currentBlock objects.StrList
		remSl := make([]uint32, 0, len(removedCols))
		for i := range removedCols {
			remSl = append(remSl, uint32(i))
		}
		r := objects.NewStrListEditor(remSl)
		for {
			minInd := 0
			var minRow []byte
			for i, chunk := range s.chunks {
				if chunkEOF[i] {
					continue
				}
				if len(chunkRows[i]) == 0 {
					n, b, err := dec.ReadBytes(chunk)
					if err == io.EOF {
						chunkEOF[i] = true
					} else if err != nil {
						errChan <- err
						return
					}
					if n > 0 {
						if n > cap(chunkRows[i]) {
							chunkRows[i] = make([]byte, n)
						}
						chunkRows[i] = chunkRows[i][:n]
						copy(chunkRows[i], b)
					} else {
						continue
					}
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

			// append min row to block
			minRow = r.RemoveFrom(minRow)
			row := dec.Decode(minRow)
			slice.CopyValuesFromIndices(row, rowPK, pkIndices)
			pkOK := pkIsDifferent(rowPK, prevRowPK)
			if pkOK {
				m := len(blk)
				blk = blk[:m+1]
				if k := len(minRow); k > cap(blk[m]) {
					blk[m] = make([]byte, k)
				} else {
					blk[m] = blk[m][:k]
				}
				copy(blk[m], minRow)
				if s.profiler != nil {
					s.profiler.Process(row)
				}
			}

			if minInd < n {
				chunkRows[minInd] = chunkRows[minInd][:0]
			} else {
				s.current = s.current[1:]
				currentBlock = nil
			}
			if len(blkPK) == 0 {
				blkPK = blkPK[:len(pkIndices)]
				copy(blkPK, rowPK)
			}
			if len(blk) == 255 {
				b := &Block{
					Offset:    offset,
					Block:     objects.CombineRowBytesIntoBlock(blk),
					PK:        make([]string, len(blkPK)),
					RowsCount: len(blk),
				}
				copy(b.PK, blkPK)
				select {
				case <-ctx.Done():
					return
				default:
					blocks <- b
				}
				offset++
				blk = blk[:0]
				blkPK = blkPK[:0]
			}
		}
		if len(blk) > 0 {
			b := &Block{
				Offset:    offset,
				Block:     objects.CombineRowBytesIntoBlock(blk),
				PK:        make([]string, len(blkPK)),
				RowsCount: len(blk),
			}
			copy(b.PK, blkPK)
			select {
			case <-ctx.Done():
				return
			default:
				blocks <- b
			}
		}
	}()
	return
}

func pkIsDifferent(pk, prevPK []string) bool {
	if prevPK == nil {
		copy(prevPK, pk)
		return true
	} else {
		if slice.StringSliceEqual(prevPK, pk) {
			return false
		}
	}
	copy(prevPK, pk)
	return true
}

func (s *Sorter) SortedRows(ctx context.Context, removedCols map[int]struct{}, errChan chan<- error) (rowsCh chan *Rows) {
	rowsCh = make(chan *Rows, 10)
	pkIndices := s.pkIndices()
	go func() {
		defer close(rowsCh)
		rows := make([][]string, 0, 255)
		offset := 0
		n := len(s.chunks)
		chunkRows := make([][]string, n)
		chunkEOF := make([]bool, n)
		dec := objects.NewStrListDecoder(false)
		SortRows(s.current, s.PK)
		chunkIdx := make([]int, n)
		pk := make([]string, len(pkIndices))
		prevPK := make([]string, len(pkIndices))
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
					} else if err != nil {
						errChan <- err
						return
					}
					if len(row) > 0 {
						chunkRows[i] = row
						chunkIdx[i]++
					} else {
						continue
					}
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
			slice.CopyValuesFromIndices(minRow, pk, pkIndices)
			pkOK := pkIsDifferent(pk, prevPK)
			if pkOK {
				rows = append(rows, s.removeCols(minRow, removedCols))
				if s.profiler != nil {
					s.profiler.Process(minRow)
				}
			}

			if minInd < n {
				chunkRows[minInd] = nil
			} else {
				s.current = s.current[1:]
			}
			if len(rows) == 255 {
				select {
				case <-ctx.Done():
					return
				default:
					rowsCh <- &Rows{
						Offset: offset,
						Rows:   rows,
					}
				}
				offset++
				rows = make([][]string, 0, 255)
			}
		}
		if len(rows) > 0 {
			select {
			case <-ctx.Done():
				return
			default:
				rowsCh <- &Rows{
					Offset: offset,
					Rows:   rows,
				}
			}
		}
	}()
	return rowsCh
}

func (s *Sorter) Close() error {
	for _, f := range s.cleanups {
		if err := f(); err != nil {
			return err
		}
	}
	return nil
}
