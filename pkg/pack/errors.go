package pack

import "fmt"

type BadRequestError struct {
	Message string
}

func (e *BadRequestError) Error() string {
	return e.Message
}

func NewBadRequestError(msg string, a ...interface{}) error {
	return &BadRequestError{Message: fmt.Sprintf(msg, a...)}
}
