package utils

import (
	"bufio"
	"context"
	"syscall"

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
	cmd.Println("")
	return val, nil
}
