package ingest

import (
	"encoding/csv"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sort"

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
	out       io.Writer
	chunks    []*os.File
	current   [][]string
	Columns   []string
	RowsCount uint32
}

func sortBlock(blk [][]string, pk []uint32) {
	sort.Slice(blk, func(i, j int) bool {
		for _, u := range pk {
			if blk[i][u] < blk[j][u] {
				return true
			}
		}
		return false
	})
}

func NewSorter(f io.ReadCloser, name string, pk []string, runSize uint64, out io.Writer) (s *Sorter, err error) {
	s = &Sorter{
		out: out,
	}
	err = s.readIntoSortedChunks(f, name, pk, runSize)
	if err != nil {
		return nil, err
	}
	return
}

func (s *Sorter) readIntoSortedChunks(f io.ReadCloser, name string, pk []string, runSize uint64) (err error) {
	if runSize == 0 {
		runSize, err = getRunSize()
		if err != nil {
			return
		}
	}
	r := csv.NewReader(f)
	s.Columns, err = r.Read()
	if err != nil {
		return
	}
	s.PK, err = slice.KeyIndices(s.Columns, pk)
	if err != nil {
		return
	}
	var row []string
	eof := false
	bar := pbar(-1, "reading "+path.Base(name), s.out)
	var size uint64
	for run := 0; ; run++ {
		for {
			row, err = r.Read()
			if err == io.EOF {
				eof = true
				break
			}
			if err != nil {
				return
			}
			size += 4
			for _, s := range row {
				size += uint64(len(s)) + 2
			}
			bar.Add(1)
			s.RowsCount++
			s.current = append(s.current, row)
			if size >= runSize {
				size = 0
				break
			}
		}
		sortBlock(s.current, s.PK)
		if eof {
			break
		} else {
			chunk, err := writeChunk(s.current)
			if err != nil {
				return err
			}
			s.chunks = append(s.chunks, chunk)
			s.current = s.current[:0]
		}
	}
	err = bar.Finish()
	if err != nil {
		return
	}
	return f.Close()
}

type block struct {
	Offset int
	Block  [][]string
	PK     []string
}

func (s *Sorter) EmitChunks(errChan chan<- error) (blocks chan *block) {
	blocks = make(chan *block, 1000)
	dec := objects.NewStrListDecoder(false)
	n := len(s.chunks)
	chunkRows := make([][]string, n)
	chunkEOF := make([]bool, n)
	go func() {
		defer close(blocks)
		blk := make([][]string, 0, 255)
		offset := 0
		var pk []string
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
			blk = append(blk, minRow)
			if len(blk) == 1 {
				pk = slice.IndicesToValues(blk[0], s.PK)
			}
			if minInd < n {
				chunkRows[minInd] = nil
			} else {
				s.current = s.current[1:]
			}
			if len(blk) == 255 {
				blocks <- &block{
					Offset: offset,
					Block:  blk,
					PK:     pk,
				}
				offset++
				blk = make([][]string, 0, 255)
			}
		}
		if len(blk) > 0 {
			blocks <- &block{
				Offset: offset,
				Block:  blk,
				PK:     pk,
			}
		}
	}()
	return blocks
}
