// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package server

import (
	"encoding/csv"
	"encoding/json"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	"github.com/wrgl/wrgl/pkg/api/payload"
)

func WriteJSON(rw http.ResponseWriter, v interface{}) {
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

func SendError(rw http.ResponseWriter, code int, message string) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(code)
	b, err := json.Marshal(&payload.Error{
		Message: message,
	})
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}

func SendHTTPError(rw http.ResponseWriter, code int) {
	SendError(rw, code, http.StatusText(code))
}

func sendCSVError(rw http.ResponseWriter, obj *csv.ParseError) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(http.StatusBadRequest)
	b, err := json.Marshal(&payload.Error{
		Message: obj.Err.Error(),
		CSV: &payload.CSVLocation{
			StartLine: obj.StartLine,
			Line:      obj.Line,
			Column:    obj.Column,
		},
	})
	if err != nil {
		panic(err)
	}
	_, err = rw.Write(b)
	if err != nil {
		panic(err)
	}
}
