// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

import (
	"io"

	"github.com/wrgl/wrgl/pkg/encoding"
	"github.com/wrgl/wrgl/pkg/encoding/objline"
	"github.com/wrgl/wrgl/pkg/misc"
)

type ColumnProfile struct {
	Name         string      `json:"name"`
	NACount      uint32      `json:"naCount"`
	Min          *float64    `json:"min,omitempty"`
	Max          *float64    `json:"max,omitempty"`
	Mean         *float64    `json:"mean,omitempty"`
	Median       *float64    `json:"median,omitempty"`
	StdDeviation *float64    `json:"stdDeviation,omitempty"`
	MinStrLen    uint16      `json:"minStrLen"`
	MaxStrLen    uint16      `json:"maxStrLen"`
	AvgStrLen    uint16      `json:"avgStrLen"`
	TopValues    ValueCounts `json:"topValues,omitempty"`
	Percentiles  []float64   `json:"percentiles,omitempty"`
}

type profileField struct {
	Name    string
	Write   func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error)
	IsEmpty func(col *ColumnProfile) bool
	Read    func(p *encoding.Parser, col *ColumnProfile) (int64, error)
}

var (
	profileFields   []*profileField
	profileFieldMap map[string]*profileField
)

func init() {
	profileFields = []*profileField{
		profileStringField("name", func(col *ColumnProfile) *string { return &col.Name }),
		profileUint32Field("naCount", func(col *ColumnProfile) *uint32 { return &col.NACount }),
		profileFloat64Field("min",
			func(col *ColumnProfile) *float64 { return col.Min },
			func(col *ColumnProfile) *float64 {
				if col.Min == nil {
					var f float64
					col.Min = &f
				}
				return col.Min
			},
		),
		profileFloat64Field("max",
			func(col *ColumnProfile) *float64 { return col.Max },
			func(col *ColumnProfile) *float64 {
				if col.Max == nil {
					var f float64
					col.Max = &f
				}
				return col.Max
			},
		),
		profileFloat64Field("mean",
			func(col *ColumnProfile) *float64 { return col.Mean },
			func(col *ColumnProfile) *float64 {
				if col.Mean == nil {
					var f float64
					col.Mean = &f
				}
				return col.Mean
			},
		),
		profileFloat64Field("median",
			func(col *ColumnProfile) *float64 { return col.Median },
			func(col *ColumnProfile) *float64 {
				if col.Median == nil {
					var f float64
					col.Median = &f
				}
				return col.Median
			},
		),
		profileFloat64Field("stdDeviation",
			func(col *ColumnProfile) *float64 { return col.StdDeviation },
			func(col *ColumnProfile) *float64 {
				if col.StdDeviation == nil {
					var f float64
					col.StdDeviation = &f
				}
				return col.StdDeviation
			},
		),
		{
			Name: "percentiles",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
				return objline.WriteBytes(NewFloatListEncoder().Encode(col.Percentiles))(w, buf)
			},
			IsEmpty: func(col *ColumnProfile) bool {
				return col.Percentiles == nil
			},
			Read: func(p *encoding.Parser, col *ColumnProfile) (n int64, err error) {
				n, col.Percentiles, err = NewFloatListDecoder(false).Read(p)
				if err != nil {
					return 0, err
				}
				return
			},
		},
		profileUint16Field("minStrLen", func(col *ColumnProfile) *uint16 { return &col.MinStrLen }),
		profileUint16Field("maxStrLen", func(col *ColumnProfile) *uint16 { return &col.MaxStrLen }),
		profileUint16Field("avgStrLen", func(col *ColumnProfile) *uint16 { return &col.AvgStrLen }),
		{
			Name: "topValues",
			Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
				return writeValueCounts(w, buf, col.TopValues)
			},
			IsEmpty: func(col *ColumnProfile) bool {
				return col.TopValues == nil
			},
			Read: func(p *encoding.Parser, col *ColumnProfile) (int64, error) {
				return readValueCounts(p, &col.TopValues)
			},
		},
	}
	profileFieldMap = map[string]*profileField{}
	for _, f := range profileFields {
		profileFieldMap[f.Name] = f
	}
}

func profileStringField(name string, getField func(col *ColumnProfile) *string) *profileField {
	return &profileField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
			return objline.WriteString(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnProfile) bool {
			return *getField(col) == ""
		},
		Read: func(p *encoding.Parser, col *ColumnProfile) (int64, error) {
			return objline.ReadString(p, getField(col))
		},
	}
}

func profileUint32Field(name string, getField func(col *ColumnProfile) *uint32) *profileField {
	return &profileField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
			return objline.WriteUint32(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnProfile) bool {
			return *getField(col) == 0
		},
		Read: func(p *encoding.Parser, col *ColumnProfile) (int64, error) {
			return objline.ReadUint32(p, getField(col))
		},
	}
}

func profileUint16Field(name string, getField func(col *ColumnProfile) *uint16) *profileField {
	return &profileField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
			return objline.WriteUint16(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnProfile) bool {
			return *getField(col) == 0
		},
		Read: func(p *encoding.Parser, col *ColumnProfile) (int64, error) {
			return objline.ReadUint16(p, getField(col))
		},
	}
}

func profileFloat64Field(name string, getField func(col *ColumnProfile) *float64, initField func(col *ColumnProfile) *float64) *profileField {
	return &profileField{
		Name: name,
		Write: func(w io.Writer, buf encoding.Bufferer, col *ColumnProfile) (int64, error) {
			return objline.WriteFloat64(w, buf, *getField(col))
		},
		IsEmpty: func(col *ColumnProfile) bool {
			return getField(col) == nil
		},
		Read: func(p *encoding.Parser, col *ColumnProfile) (int64, error) {
			f := initField(col)
			return objline.ReadFloat64(p, f)
		},
	}
}

type TableProfile struct {
	Version   uint32           `json:"-"`
	RowsCount uint32           `json:"rowsCount"`
	Columns   []*ColumnProfile `json:"columns"`
}

func (t *TableProfile) WriteTo(w io.Writer) (total int64, err error) {
	buf := misc.NewBuffer(nil)
	names := make([]string, len(profileFields))
	for i, f := range profileFields {
		names[i] = f.Name
	}
	for _, field := range []fieldEncode{
		{"version", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, t.Version)
		}},
		{"fields", objline.WriteBytes(NewStrListEncoder(true).Encode(names))},
		{"rowsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, t.RowsCount)
		}},
		{"colsCount", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			return objline.WriteUint32(w, buf, uint32(len(t.Columns)))
		}},
		{"columns", func(w io.Writer, buf encoding.Bufferer) (n int64, err error) {
			for _, col := range t.Columns {
				for j, field := range profileFields {
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

func (t *TableProfile) ReadFrom(r io.Reader) (total int64, err error) {
	parser := encoding.NewParser(r)
	var fields []string
	var count uint32
	for _, f := range []fieldDecode{
		{"version", func(p *encoding.Parser) (int64, error) {
			return objline.ReadUint32(p, &t.Version)
		}},
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
			t.Columns = make([]*ColumnProfile, count)
			for i := uint32(0); i < count; i++ {
				t.Columns[i] = &ColumnProfile{}
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
					if sf, ok := profileFieldMap[field]; !ok {
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
