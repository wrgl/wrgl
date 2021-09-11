package apiclient

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/wrgl/core/pkg/api/payload"
)

type HTTPError struct {
	Code    int
	RawBody []byte
	Body    *payload.Error
}

func NewHTTPError(resp *http.Response) *HTTPError {
	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		panic(err)
	}
	obj := &HTTPError{
		Code:    resp.StatusCode,
		RawBody: b,
	}
	if s := resp.Header.Get("Content-Type"); s == CTJSON {
		obj.Body = &payload.Error{}
		if err := json.Unmarshal(b, obj.Body); err != nil {
			panic(err)
		}
	} else {
		obj.RawBody = b
	}
	return obj
}

func (obj *HTTPError) Error() string {
	b := obj.RawBody
	var err error
	if obj.Body != nil {
		b, err = json.Marshal(obj.Body)
		if err != nil {
			panic(err)
		}
	}
	return fmt.Sprintf("status %d: %s", obj.Code, string(b))
}
