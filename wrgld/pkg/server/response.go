package server

import (
	"encoding/csv"
	"encoding/json"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
)

func WriteJSON(rw http.ResponseWriter, r *http.Request, v interface{}) {
	setResponseInfo(r, v)
	rw.Header().Set("Content-Type", api.CTJSON)
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}

func SendError(rw http.ResponseWriter, r *http.Request, code int, message string) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(code)
	errPayload := &payload.Error{
		Message: message,
	}
	setResponseInfo(r, errPayload)
	b, err := json.Marshal(errPayload)
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}

func SendHTTPError(rw http.ResponseWriter, r *http.Request, code int) {
	SendError(rw, r, code, http.StatusText(code))
}

func sendCSVError(rw http.ResponseWriter, r *http.Request, obj *csv.ParseError) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(http.StatusBadRequest)
	errPayload := &payload.Error{
		Message: obj.Err.Error(),
		CSV: &payload.CSVLocation{
			StartLine: obj.StartLine,
			Line:      obj.Line,
			Column:    obj.Column,
		},
	}
	setResponseInfo(r, errPayload)
	b, err := json.Marshal(errPayload)
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}
