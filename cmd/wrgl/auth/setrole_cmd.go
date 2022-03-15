// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package auth

import (
	"fmt"
	"sort"
	"strings"

	"github.com/spf13/cobra"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/auth"
	authfs "github.com/wrgl/wrgl/pkg/auth/fs"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
)

var roles = map[string][]string{
	"editor": {
		auth.ScopeRepoWrite,
		auth.ScopeRepoWriteConfig,
		auth.ScopeRepoRead,
		auth.ScopeRepoReadConfig,
	},
	"viewer": {
		auth.ScopeRepoRead,
	},
}

func validRoles() []string {
	names := make([]string, 0, len(roles))
	for s := range roles {
		names = append(names, s)
	}
	sort.Strings(names)
	return names
}

func rolesString() string {
	names := validRoles()
	sb := &strings.Builder{}
	for _, role := range names {
		fmt.Fprintf(sb, "  - %s {%s}\n", role, strings.Join(roles[role], ", "))
	}
	return sb.String()
}

func setroleCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   fmt.Sprintf("set-role { EMAIL | %s } ROLE", auth.Anyone),
		Short: "Assign scopes associated with a role to a user.",
		Long: fmt.Sprintf(
			"Assign scopes associated with a role to a user. Previously assigned scopes that are not included in ROLE are removed. Valid roles and their associated scopes are:\n%s",
			rolesString(),
		),
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "Give role viewer to a user",
				Line:    "wrgl auth set-role user@mail.com viewer",
			},
			{
				Comment: "Give role editor to a user",
				Line:    "wrgl auth set-role user@mail.com editor",
			},
			{
				Comment: "Give role viewer to anonymous users",
				Line:    fmt.Sprintf("wrgl auth set-role %s viewer", auth.Anyone),
			},
		}),
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			user, role := args[0], args[1]
			dir := utils.MustWRGLDir(cmd)
			cs := conffs.NewStore(dir, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return err
			}
			rd := utils.GetRepoDir(cmd)
			defer rd.Close()
			if err := ensureUserExist(rd, c, user); err != nil {
				return err
			}
			authzS, err := authfs.NewAuthzStore(rd)
			if err != nil {
				return err
			}
			defer authzS.Close()

			scopes, ok := roles[role]
			if !ok {
				return fmt.Errorf("unrecognized role %q, valid roles are: %s", role, strings.Join(validRoles(), ", "))
			}
			sort.Strings(scopes)
			sl, err := authzS.ListPolicies(user)
			if err != nil {
				return err
			}
			for _, s := range sl {
				if idx := sort.Search(len(scopes), func(i int) bool {
					return scopes[i] >= s
				}); idx >= len(scopes) || s != scopes[idx] {
					if err := authzS.RemovePolicy(user, s); err != nil {
						return err
					}
				}
			}
			for _, scope := range scopes {
				if err := authzS.AddPolicy(user, scope); err != nil {
					return err
				}
			}
			return authzS.Flush()
		},
	}
	return cmd
}
