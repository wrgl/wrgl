// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
)

type ColumnSummary struct {
	Name        string      `json:"name"`
	NullCount   uint32      `json:"nullCount"`
	IsNumber    bool        `json:"isNumber,omitempty"`
	Min         *float64    `json:"min,omitempty"`
	Max         *float64    `json:"max,omitempty"`
	Mean        *float64    `json:"mean,omitempty"`
	Median      *float64    `json:"median,omitempty"`
	Mode        *float64    `json:"mode,omitempty"`
	AvgStrLen   uint16      `json:"avgStrLen"`
	TopValues   ValueCounts `json:"topValues,omitempty"`
	Percentiles []float64   `json:"percentiles,omitempty"`
}

type summaryField struct {
	Name    string
	Write   func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error)
	IsEmpty func(col *ColumnSummary) bool
	Read    func(p *encoding.Parser, col *ColumnSummary) (int64, error)
}

var (
	summaryFields   []*summaryField
	summaryFieldMap map[string]*summaryField
)

func init() {
	summaryFields = []*summaryField{
		summaryStringField("name", func(col *ColumnSummary) *string { return &col.Name }),
		summaryUint32Field("nullCount", func(col *ColumnSummary) *uint32 { return &col.NullCount }),
		summaryBoolField("isNumber", func(col *ColumnSummary) *bool { return &col.IsNumber }),
		summaryFloat64Field("min",
			func(col *ColumnSummary) *float64 { return col.Min },
			func(col *ColumnSummary) *float64 {
				if col.Min == nil {
					var f float64
					col.Min = &f
				}
				return col.Min
			},
		),
		summaryFloat64Field("max",
			func(col *ColumnSummary) *float64 { return col.Max },
			func(col *ColumnSummary) *float64 {
				if col.Max == nil {
					var f float64
					col.Max = &f
				}
				return col.Max
			},
		),
		summaryFloat64Field("mean",
			func(col *ColumnSummary) *float64 { return col.Mean },
			func(col *ColumnSummary) *float64 {
				if col.Mean == nil {
					var f float64
					col.Mean = &f
				}
				return col.Mean
			},
		),
		summaryFloat64Field("median",
			func(col *ColumnSummary) *float64 { return col.Median },
			func(col *ColumnSummary) *float64 {
				if col.Median == nil {
					var f float64
					col.Median = &f
				}
				return col.Median
			},
		),
		summaryFloat64Field("mode",
			func(col *ColumnSummary) *float64 { return col.Mode },
			func(col *ColumnSummary) *float64 {
				if col.Mode == nil {
					var f float64
					col.Mode = &f
				}
				return col.Mode
			},
		),
		{
			Name: "percentiles",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return objline.WriteBytes(NewFloatListEncoder().Encode(col.Percentiles))(w, buf)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.Percentiles == nil
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (n int64, err error) {
				n, col.Percentiles, err = NewFloatListDecoder(false).Read(p)
				if err != nil {
					return 0, err
				}
				return
			},
		},
		summaryUint16Field("avgStrLen", func(col *ColumnSummary) *uint16 { return &col.AvgStrLen }),
		{
			Name: "topValues",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
				return writeValueCounts(w, buf, col.TopValues)
			},
			IsEmpty: func(col *ColumnSummary) bool {
				return col.TopValues == nil
			},
			Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
				return readValueCounts(p, &col.TopValues)
			},
		},
	}
	summaryFieldMap = map[string]*summaryField{}
	for _, f := range summaryFields {
		summaryFieldMap[f.Name] = f
	}
}

func summaryStringField(name string, getField func(col *ColumnSummary) *string) *summaryField {
	return &summaryField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
			return objline.WriteString(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnSummary) bool {
			return *getField(col) == ""
		},
		Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
			return objline.ReadString(p, getField(col))
		},
	}
}

func summaryUint32Field(name string, getField func(col *ColumnSummary) *uint32) *summaryField {
	return &summaryField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
			return objline.WriteUint32(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnSummary) bool {
			return *getField(col) == 0
		},
		Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
			return objline.ReadUint32(p, getField(col))
		},
	}
}

func summaryUint16Field(name string, getField func(col *ColumnSummary) *uint16) *summaryField {
	return &summaryField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
			return objline.WriteUint16(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnSummary) bool {
			return *getField(col) == 0
		},
		Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
			return objline.ReadUint16(p, getField(col))
		},
	}
}

func summaryFloat64Field(name string, getField func(col *ColumnSummary) *float64, initField func(col *ColumnSummary) *float64) *summaryField {
	return &summaryField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
			return objline.WriteFloat64(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnSummary) bool {
			return getField(col) == nil
		},
		Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
			f := initField(col)
			return objline.ReadFloat64(p, f)
		},
	}
}

func summaryBoolField(name string, getField func(col *ColumnSummary) *bool) *summaryField {
	return &summaryField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnSummary) (int64, error) {
			return objline.WriteBool(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnSummary) bool {
			return !*getField(col)
		},
		Read: func(p *encoding.Parser, col *ColumnSummary) (int64, error) {
			return objline.ReadBool(p, getField(col))
		},
	}
}

type TableSummary struct {
	RowsCount uint32           `json:"rowsCount"`
	Columns   []*ColumnSummary `json:"columns"`
}

func (t *TableSummary) WriteTo(w io.Writer) (total int64, err error) {
	buf := misc.NewBuffer(nil)
	names := make([]string, len(summaryFields))
	for i, f := range summaryFields {
		names[i] = f.Name
	}
	for _, field := range []fieldEncode{
		{"fields", objline.WriteBytes(NewStrListEncoder(true).Encode(names))},
		{"rowsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, t.RowsCount)
		}},
		{"colsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, uint32(len(t.Columns)))
		}},
		{"columns", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			for _, col := range t.Columns {
				for j, field := range summaryFields {
					// skip empty field
					if field.IsEmpty(col) {
						continue
					}
					// write the field index
					l, err := objline.WriteUint16(w, buf, uint16(j+1))
					if err != nil {
						return 0, err
					}
					n += l
					// write the field content
					l, err = field.Write(w, buf, col)
					if err != nil {
						return 0, err
					}
					n += l
				}
				// mark the end of a column
				l, err := objline.WriteUint16(w, buf, 0)
				if err != nil {
					return 0, err
				}
				n += l
			}
			return n, nil
		}},
	} {
		n, err := objline.WriteField(w, buf, field.label, field.f)
		if err != nil {
			return 0, err
		}
		total += n
	}
	return total, nil
}

func (t *TableSummary) ReadFrom(r io.Reader) (total int64, err error) {
	parser := encoding.NewParser(r)
	var fields []string
	var count uint32
	for _, f := range []fieldDecode{
		{"fields", func(p *encoding.Parser) (n int64, err error) {
			n, fields, err = NewStrListDecoder(false).Read(p)
			if err != nil {
				return 0, err
			}
			return n, nil
		}},
		{"rowsCount", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &t.RowsCount)
		}},
		{"colsCount", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &count)
		}},
		{"columns", func(p *encoding.Parser) (n int64, err error) {
			var j uint16
			nFields := uint16(len(fields))
			t.Columns = make([]*ColumnSummary, count)
			for i := uint32(0); i < count; i++ {
				t.Columns[i] = &ColumnSummary{}
				for {
					l, err := objline.ReadUint16(p, &j)
					if err != nil {
						return 0, err
					}
					n += l
					if j == 0 {
						break
					}
					if j > nFields {
						return 0, p.ParseError("invalid field index %d >= %d", j, nFields)
					}
					field := fields[j-1]
					if sf, ok := summaryFieldMap[field]; !ok {
						return 0, p.ParseError("summary field %q not found", field)
					} else {
						l, err = sf.Read(p, t.Columns[i])
						if err != nil {
							return 0, err
						}
						n += l
					}
				}
			}
			return
		}},
	} {
		n, err := objline.ReadField(parser, f.label, f.f)
		if err != nil {
			return 0, err
		}
		total += int64(n)
	}
	return
}
