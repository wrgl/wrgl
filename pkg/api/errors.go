// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package api

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
