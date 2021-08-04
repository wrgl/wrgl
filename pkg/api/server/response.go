// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package apiserver

import (
	"encoding/json"
	"net/http"

	"github.com/wrgl/core/pkg/api"
)

func writeJSON(rw http.ResponseWriter, v interface{}) {
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
