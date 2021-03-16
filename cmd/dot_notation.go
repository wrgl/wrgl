package main

import (
	"fmt"
	"reflect"
	"strings"
)

func getFieldValue(s interface{}, prop string) (reflect.Value, error) {
	props := strings.Split(prop, ".")
	v := reflect.ValueOf(s)
	t := reflect.TypeOf(s)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
		v = v.Elem()
	}
	if t.Kind() != reflect.Struct {
		return reflect.Value{}, fmt.Errorf("interface isn't a struct")
	}
	fv := v
	for _, p := range props {
		name := strings.ToUpper(string(p[0])) + p[1:]
		sf, ok := t.FieldByName(name)
		if !ok {
			return reflect.Value{}, fmt.Errorf(`field "%s" not found`, name)
		}
		t = sf.Type
		fv = fv.FieldByName(name)
	}
	return fv, nil
}

func GetWithDotNotation(s interface{}, prop string) (interface{}, error) {
	fv, err := getFieldValue(s, prop)
	if err != nil {
		return nil, err
	}
	return fv.Interface(), nil
}

func SetWithDotNotation(s interface{}, prop string, val interface{}) error {
	fv, err := getFieldValue(s, prop)
	if err != nil {
		return err
	}
	fv.Set(reflect.ValueOf(val))
	return nil
}
