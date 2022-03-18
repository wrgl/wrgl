package objects

import (
	"io"
	"time"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
)

type TransactionStatus string

const (
	TSInProgress TransactionStatus = "inprogress"
	TSCommitted  TransactionStatus = "committed"
)

type Transaction struct {
	Status TransactionStatus
	Begin  time.Time
	End    time.Time
}

func (t *Transaction) WriteTo(w io.Writer) (int64, error) {
	buf := misc.NewBuffer(nil)
	fields := []fieldEncode{
		{"status", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteString(w, buf, string(t.Status))
		}},
		{"begin", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteTime(w, buf, t.Begin)
		}},
		{"end", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteTime(w, buf, t.End)
		}},
	}
	var total int64
	for _, l := range fields {
		n, err := objline.WriteField(w, buf, l.label, l.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (t *Transaction) ReadFrom(r io.Reader) (int64, error) {
	parser := encoding.NewParser(r)
	var total int64
	for _, l := range []fieldDecode{
		{"status", func(p *encoding.Parser) (int64, error) {
			return objline.ReadString(p, (*string)(&t.Status))
		}},
		{"begin", func(p *encoding.Parser) (int64, error) {
			return objline.ReadTime(p, &t.Begin)
		}},
		{"end", func(p *encoding.Parser) (int64, error) {
			return objline.ReadTime(p, &t.End)
		}},
	} {
		n, err := objline.ReadField(parser, l.label, l.f)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return total, nil
}

func ReadTransactionFrom(r io.Reader) (int64, *Transaction, error) {
	t := &Transaction{}
	n, err := t.ReadFrom(r)
	return n, t, err
}
