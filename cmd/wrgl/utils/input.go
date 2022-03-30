// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package utils

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"syscall"
	"unicode/utf8"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type promptKey struct{}

func SetPromptValues(ctx context.Context, values []string) context.Context {
	return context.WithValue(ctx, promptKey{}, &values)
}

func dequeuePromptValues(ctx context.Context) string {
	if i := ctx.Value(promptKey{}); i != nil {
		sl := i.(*[]string)
		if len(*sl) > 0 {
			s := (*sl)[0]
			*sl = (*sl)[1:]
			return s
		}
	}
	return ""
}

func PromptForPassword(cmd *cobra.Command) (password string, err error) {
	if s := dequeuePromptValues(cmd.Context()); s != "" {
		return s, nil
	}
	cmd.Print("Password: ")
	bytePassword, err := term.ReadPassword(syscall.Stdin)
	if err != nil {
		return "", err
	}
	cmd.Println("")
	return string(bytePassword), nil
}

func Prompt(cmd *cobra.Command, name string) (value string, err error) {
	if s := dequeuePromptValues(cmd.Context()); s != "" {
		return s, nil
	}
	cmd.Printf("%s: ", name)
	r := bufio.NewReader(cmd.InOrStdin())
	val, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.Trim(val, "\n "), nil
}

func GetRuneFromFlag(cmd *cobra.Command, flag string) (rune, error) {
	s, err := cmd.Flags().GetString(flag)
	if err != nil {
		return 0, err
	}
	if s != "" {
		r, size := utf8.DecodeRuneInString(s)
		if size > 0 {
			return r, nil
		}
		return 0, fmt.Errorf("error reading rune from flag %q: could not decode rune in %q", flag, s)
	}
	return 0, nil
}
