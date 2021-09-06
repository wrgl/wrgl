package utils

import (
	"context"
	"syscall"

	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type passwordKey struct{}

func SetPassword(ctx context.Context, password string) context.Context {
	return context.WithValue(ctx, passwordKey{}, password)
}

func PromptForPassword(cmd *cobra.Command) (password string, err error) {
	if i := cmd.Context().Value(passwordKey{}); i != nil {
		return i.(string), nil
	}
	cmd.Print("Password: ")
	bytePassword, err := term.ReadPassword(syscall.Stdin)
	if err != nil {
		return "", err
	}
	return string(bytePassword), nil
}
