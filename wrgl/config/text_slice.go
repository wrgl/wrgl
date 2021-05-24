package config

import (
	"encoding"
	"fmt"
	"reflect"
)

type TextSlice struct {
	Value      reflect.Value
	isStrSlice bool
}

func unmarshalText(t reflect.Type, s string) (v reflect.Value, err error) {
	field := reflect.New(t).Elem()
	if field.Kind() == reflect.Ptr {
		field = reflect.New(field.Type().Elem())
	}
	o := field.Interface().(encoding.TextUnmarshaler)
	err = o.UnmarshalText([]byte(s))
	if err != nil {
		return
	}
	v = reflect.ValueOf(o)
	return
}

func isSliceOfTextMarshaler(t reflect.Type) bool {
	return t.Kind() == reflect.Slice &&
		t.Elem().Implements(reflect.TypeOf(new(encoding.TextUnmarshaler)).Elem()) &&
		t.Elem().Implements(reflect.TypeOf(new(encoding.TextMarshaler)).Elem())
}

func MustBeTextSlice(obj interface{}) *TextSlice {
	sl, ok := ToTextSlice(obj)
	if !ok {
		panic(fmt.Errorf("type %v is not a text slice", reflect.TypeOf(obj)))
	}
	return sl
}

func ToTextSlice(obj interface{}) (sl *TextSlice, ok bool) {
	sl = &TextSlice{
		Value: reflect.ValueOf(obj),
	}
	if _, ok = obj.([]string); ok {
		sl.isStrSlice = true
		return
	}
	ok = isSliceOfTextMarshaler(sl.Value.Type())
	if !ok {
		return nil, ok
	}
	return
}

func TextSliceFromStrSlice(t reflect.Type, strs []string) (sl *TextSlice, err error) {
	if t.Kind() == reflect.Slice && t.Elem().Kind() == reflect.String {
		sl = &TextSlice{
			Value:      reflect.ValueOf(strs),
			isStrSlice: true,
		}
		return
	}
	if !isSliceOfTextMarshaler(t) {
		return nil, fmt.Errorf("type %v does not implement encoding.TextUnmarshaler and encoding.TextMarshaler", t.Elem())
	}
	sl = &TextSlice{
		Value: reflect.New(t).Elem(),
	}
	err = sl.Append(strs...)
	if err != nil {
		return nil, err
	}
	return
}

func (sl *TextSlice) Len() int {
	return sl.Value.Len()
}

func (sl *TextSlice) Set(i int, s string) (err error) {
	if sl.isStrSlice {
		sl.Value.Index(i).Set(reflect.ValueOf(s))
		return nil
	}
	o, err := unmarshalText(sl.Value.Type().Elem(), s)
	if err != nil {
		return
	}
	sl.Value.Index(i).Set(o)
	return nil
}

func (sl *TextSlice) Get(i int) (s string, err error) {
	e := sl.Value.Index(i)
	if sl.isStrSlice {
		return e.String(), nil
	}
	o := e.Interface().(encoding.TextMarshaler)
	b, err := o.MarshalText()
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (sl *TextSlice) Append(strs ...string) (err error) {
	n := sl.Len()
	m := len(strs)
	v := reflect.MakeSlice(sl.Value.Type(), n+m, n+m)
	reflect.Copy(v, sl.Value)
	sl.Value = v
	for i, s := range strs {
		err := sl.Set(n+i, s)
		if err != nil {
			return err
		}
	}
	return nil
}

func (sl *TextSlice) ToStringSlice() ([]string, error) {
	n := sl.Len()
	result := make([]string, n)
	if sl.isStrSlice {
		copy(result, sl.Value.Interface().([]string))
		return result, nil
	}
	for i := 0; i < n; i++ {
		s, err := sl.Get(i)
		if err != nil {
			return nil, err
		}
		result[i] = s
	}
	return result, nil
}
