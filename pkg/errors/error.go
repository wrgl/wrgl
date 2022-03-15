// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package errors

import (
	"errors"
	"fmt"
)

type Error struct {
	msg string
	err error
}

func (e *Error) Error() string {
	return fmt.Sprintf("%s: %v", e.msg, e.err)
}

func (e *Error) Unwrap() error {
	return e.err
}

func Wrap(msg string, err error) *Error {
	return &Error{msg, err}
}

func Unwrap(err error) error {
	return errors.Unwrap(err)
}

func Contains(err error, v interface{}) bool {
	var s string
	if v == nil {
		return err == nil
	}
	if err == nil {
		return false
	}
	switch t := v.(type) {
	case string:
		s = t
	case error:
		s = t.Error()
	default:
		return false
	}
	for {
		if err.Error() == s {
			return true
		}
		err = Unwrap(err)
		if err == nil {
			return false
		}
	}
}
