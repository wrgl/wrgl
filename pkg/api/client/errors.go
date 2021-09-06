package apiclient

import (
	"fmt"
	"strings"
)

type HTTPError struct {
	Code    int
	Message string
}

func NewHTTPError(code int, message string) *HTTPError {
	return &HTTPError{
		Code:    code,
		Message: strings.TrimSpace(message),
	}
}

func (err *HTTPError) Error() string {
	return fmt.Sprintf("status %d: %s", err.Code, err.Message)
}
