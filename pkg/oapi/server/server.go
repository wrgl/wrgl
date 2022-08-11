//go:generate oapi-codegen --config server.cfg.yaml ../oapi.yaml

package wrgloapiserver

import "time"

func StrPtr(s string) *string {
	var r = s
	return &r
}

func StrSlicePtr(sl []string) *[]string {
	var r = make([]string, len(sl))
	copy(r, sl)
	return &r
}

func BoolPtr(b bool) *bool {
	var r = b
	return &r
}

func TimePtr(t time.Time) *time.Time {
	r := &time.Time{}
	*r = t
	return r
}

func ObjectHashFromBytes(b []byte) *ObjectHash {
	h := &ObjectHash{}
	copy(h[:], b)
	return h
}

func Uint32ToIntPtr(v uint32) *int {
	var r = int(v)
	return &r
}

func Uint32ToIntSlice(sl []uint32) []int {
	r := make([]int, len(sl))
	for i, v := range sl {
		r[i] = int(v)
	}
	return r
}

func Float32Ptr(v float32) *float32 {
	r := v
	return &r
}
