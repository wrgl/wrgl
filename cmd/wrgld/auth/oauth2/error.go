package authoauth2

import (
	"fmt"
	"net/http"

	"github.com/wrgl/wrgl/pkg/api"
	apiserver "github.com/wrgl/wrgl/pkg/api/server"
)

type ResponseError interface {
	error
	WriteResponse(rw http.ResponseWriter)
}

type Oauth2Error struct {
	Err              string `json:"error,omitempty"`
	ErrorDescription string `json:"error_description,omitempty"`
}

func (e *Oauth2Error) WriteResponse(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(http.StatusBadRequest)
	apiserver.WriteJSON(rw, e)
}

func (e *Oauth2Error) Error() string {
	return fmt.Sprintf("Oauth2Error %s: %s", e.Err, e.ErrorDescription)
}

type UnauthorizedError struct {
	Message      string `json:"message,omitempty"`
	CurrentScope string `json:"current_scope,omitempty"`
	MissingScope string `json:"missing_scope,omitempty"`
}

func (e *UnauthorizedError) WriteResponse(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(http.StatusUnauthorized)
	apiserver.WriteJSON(rw, e)
}

func (e *UnauthorizedError) Error() string {
	return fmt.Sprintf("UnauthorizedError: %s {current_scope: %s, missing_scope: %s}", e.Message, e.CurrentScope, e.MissingScope)
}

type HTTPError struct {
	Code    int    `json:"-"`
	Message string `json:"message"`
}

func (e *HTTPError) WriteResponse(rw http.ResponseWriter) {
	rw.Header().Set("Content-Type", api.CTJSON)
	rw.WriteHeader(e.Code)
	apiserver.WriteJSON(rw, e)
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTPError: %d %s", e.Code, e.Message)
}

func handleError(rw http.ResponseWriter, err error) {
	if v, ok := err.(ResponseError); ok {
		v.WriteResponse(rw)
	} else if v, ok := err.(error); ok {
		panic(v)
	}
}
