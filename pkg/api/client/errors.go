// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package apiclient

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/wrgl/wrgl/pkg/api/payload"
	"github.com/wrgl/wrgl/pkg/objects"
	"github.com/wrgl/wrgl/pkg/ref"
)

type HTTPError struct {
	Code    int
	RawBody []byte
	Body    *payload.Error
}

func (err *HTTPError) Is(target error) bool {
	if v, ok := target.(*HTTPError); ok {
		if v.Code != err.Code || !bytes.Equal(v.RawBody, err.RawBody) {
			return false
		}
		return v.Body.Equal(err.Body)
	}
	return false
}

func NewHTTPError(resp *http.Response) *HTTPError {
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
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
	return fmt.Sprintf("status %d: %s", obj.Code, strings.TrimSpace(string(b)))
}

type ShallowCommitError struct {
	ComSumMap map[string]struct{}
	TableSums map[string][][]byte
}

func NewShallowCommitError(db objects.Store, rs ref.Store, coms []*objects.Commit) *ShallowCommitError {
	e := &ShallowCommitError{
		ComSumMap: map[string]struct{}{},
		TableSums: map[string][][]byte{},
	}
	for _, com := range coms {
		if !objects.TableExist(db, com.Table) {
			e.ComSumMap[string(com.Sum)] = struct{}{}
			rem, err := FindRemoteFor(db, rs, com.Sum)
			if err != nil {
				panic(err)
			}
			if rem == "" {
				panic(fmt.Errorf("no remote found for table %x", com.Table))
			}
			e.TableSums[rem] = append(e.TableSums[rem], com.Table)
		}
	}
	if len(e.ComSumMap) > 0 {
		return e
	}
	return nil
}

func (e *ShallowCommitError) Error() string {
	comSums := make([]string, 0, len(e.ComSumMap))
	for v := range e.ComSumMap {
		comSums = append(comSums, hex.EncodeToString([]byte(v)))
	}
	cmds := make([]string, 0, len(e.TableSums))
	for rem, sl := range e.TableSums {
		tblSums := make([]string, len(sl))
		for i, v := range sl {
			tblSums[i] = hex.EncodeToString(v)
		}
		cmds = append(cmds, fmt.Sprintf("wrgl fetch tables %s %s", rem, strings.Join(tblSums, " ")))
	}
	if len(comSums) == 1 {
		return fmt.Sprintf(
			"commit %s is shallow\nrun this command to fetch their content:\n  %s",
			strings.Join(comSums, ", "),
			strings.Join(cmds, "\n  "),
		)
	}
	return fmt.Sprintf(
		"commits %s are shallow\nrun this command to fetch their content:\n  %s",
		strings.Join(comSums, ", "),
		strings.Join(cmds, "\n  "),
	)
}

func (e *ShallowCommitError) CommitSums() [][]byte {
	comSums := make([][]byte, 0, len(e.ComSumMap))
	for v := range e.ComSumMap {
		comSums = append(comSums, []byte(v))
	}
	return comSums
}

func UnwrapHTTPError(err error) *HTTPError {
	werr := err
	for {
		if v, ok := werr.(*HTTPError); ok {
			return v
		}
		werr = errors.Unwrap(werr)
		if werr == nil {
			return nil
		}
	}
}

func IsHTTPError(err error, statusCode int, message string) bool {
	var herr *HTTPError
	if errors.As(err, &herr) {
		return herr.Code == statusCode && herr.Body.Message == message
	}
	return false
}
