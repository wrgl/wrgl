package auth

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/local"
)

var validScopes = map[string]string{
	auth.ScopeRepoRead:        "covers view-only actions such as fetch, diff, etc...",
	auth.ScopeRepoReadConfig:  "covers read config action",
	auth.ScopeRepoWrite:       "covers write actions such as push, commit, etc...",
	auth.ScopeRepoWriteConfig: "covers write config action",
}

func maxScopeLength() int {
	n := 0
	for scope := range validScopes {
		if len(scope) > n {
			n = len(scope)
		}
	}
	return n
}

func allScopesString(indent int, withDesc bool) string {
	sl := []string{}
	spaces := strings.Repeat(" ", indent)
	maxlen := maxScopeLength()
	for scope, desc := range validScopes {
		var s string
		if withDesc {
			s = fmt.Sprintf("%s%s%s\t%s", spaces, scope, strings.Repeat(" ", maxlen-len(scope)), desc)
		} else {
			s = fmt.Sprintf("%s%s", spaces, scope)
		}
		sl = append(sl, s)
	}
	sort.Strings(sl)
	return strings.Join(sl, "\n")
}

func ensureUserExist(rd *local.RepoDir, c *conf.Config, email string) error {
	if email == auth.Anyone {
		return nil
	}
	authnS, err := authfs.NewAuthnStore(rd, c.TokenDuration())
	if err != nil {
		return err
	}
	defer authnS.Close()
	if !authnS.Exist(email) {
		return UserNotFoundErr(email)
	}
	return nil
}

func addscopeCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("add-scope { EMAIL | %s } SCOPE...", auth.Anyone),
		Short: "Add one or more scopes for a user.",
		Long:  "Add one or more scopes for a user. Scopes represent what actions are allowed via the Wrgld HTTP API for a users. Valid scopes are:\n" + allScopesString(2, true),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "authorize user to fetch & push data",
				Line:    fmt.Sprintf("wrgl auth add-scope user@email.com %s %s", auth.ScopeRepoRead, auth.ScopeRepoWrite),
			},
			{
				Comment: "authorize a user to do everything",
				Line:    "wrgl auth add-scope user@email.com --all",
			},
			{
				Comment: "allow everyone to fetch data (make this repo public)",
				Line:    fmt.Sprintf("wrgl auth add-scope %s %s", auth.Anyone, auth.ScopeRepoRead),
			},
		}),
		Args: cobra.ArbitraryArgs,
		RunE: func(cmd *cobra.Command, args []string) error {
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := ensureUserExist(rd, c, args[0]); err != nil {
				return err
			}
			authzS, err := authfs.NewAuthzStore(rd)
			if err != nil {
				return err
			}
			defer authzS.Close()
			all, err := cmd.Flags().GetBool("all")
			if err != nil {
				return err
			}
			var scopes []string
			if all {
				scopes = []string{
					auth.ScopeRepoRead,
					auth.ScopeRepoReadConfig,
					auth.ScopeRepoWrite,
					auth.ScopeRepoWriteConfig,
				}
			} else {
				for _, scope := range args[1:] {
					if _, ok := validScopes[scope]; !ok {
						return InvalidScopeErr(scope)
					}
					scopes = append(scopes, scope)
				}
			}
			for _, scope := range scopes {
				if err := authzS.AddPolicy(args[0], scope); err != nil {
					return err
				}
			}
			return authzS.Flush()
		},
	}
	cmd.Flags().Bool("all", false, "add all scopes to user")
	return cmd
}
