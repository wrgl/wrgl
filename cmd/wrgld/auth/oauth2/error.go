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
	WriteHTMLResponse(rw http.ResponseWriter)
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

func (e *Oauth2Error) WriteHTMLResponse(rw http.ResponseWriter) {
	writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: fmt.Sprintf("%s: %s", e.Err, e.ErrorDescription)})
}

func (e *Oauth2Error) Error() string {
	return fmt.Sprintf("Oauth2Error %s: %s", e.Err, e.ErrorDescription)
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

func (e *HTTPError) WriteHTMLResponse(rw http.ResponseWriter) {
	writeErrorHTML(rw, http.StatusBadRequest, &errorTmplData{ErrorMessage: e.Message})
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTPError: %d %s", e.Code, e.Message)
}

func outputError(rw http.ResponseWriter, err error) {
	if v, ok := err.(ResponseError); ok {
		v.WriteResponse(rw)
	} else {
		panic(err)
	}
}

func outputHTMLError(rw http.ResponseWriter, err error) {
	if v, ok := err.(ResponseError); ok {
		v.WriteHTMLResponse(rw)
	} else {
		panic(err)
	}
}
