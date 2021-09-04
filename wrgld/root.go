// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import (
	"fmt"
	"log"
	"time"

	"github.com/spf13/cobra"
	authfs "github.com/wrgl/core/pkg/auth/fs"
	conffs "github.com/wrgl/core/pkg/conf/fs"
	"github.com/wrgl/core/pkg/local"
)

func newRootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wrgld [WRGL_DIR]",
		Short: "Starts an HTTP server providing access to the repository at <current_dir>/.wrgl or WRGL_DIR folder if it is given.",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) (err error) {
			var dir string
			if len(args) > 0 {
				dir = args[0]
			} else {
				dir, err = local.FindWrglDir()
				if err != nil {
					return err
				}
				if dir == "" {
					return fmt.Errorf("repository not initialized in current directory. Initialize with command:\n  wrgl init")
				}
				log.Printf("repository found at %s\n", dir)
			}
			port, err := cmd.Flags().GetInt("port")
			if err != nil {
				return
			}
			readTimeout, err := cmd.Flags().GetDuration("read-timeout")
			if err != nil {
				return
			}
			writeTimeout, err := cmd.Flags().GetDuration("write-timeout")
			if err != nil {
				return
			}
			badgerLogInfo, err := cmd.Flags().GetBool("badger-log-info")
			if err != nil {
				return
			}
			badgerLogDebug, err := cmd.Flags().GetBool("badger-log-debug")
			if err != nil {
				return
			}
			rd := local.NewRepoDir(dir, badgerLogInfo, badgerLogDebug)
			objstore, err := rd.OpenObjectsStore()
			if err != nil {
				return
			}
			refstore := rd.OpenRefStore()
			cs := conffs.NewStore(rd.FullPath, conffs.AggregateSource, "")
			c, err := cs.Open()
			if err != nil {
				return
			}
			var tokDuration time.Duration
			if c.Auth != nil && c.Auth.TokenDuration > 0 {
				tokDuration = c.Auth.TokenDuration
			}
			authnS, err := authfs.NewAuthnStore(rd.FullPath, tokDuration)
			if err != nil {
				return
			}
			authzS, err := authfs.NewAuthzStore(rd.FullPath)
			if err != nil {
				return
			}
			server := NewServer(authnS, authzS, objstore, refstore, cs, readTimeout, writeTimeout)
			defer server.Close()
			return server.Start(fmt.Sprintf(":%d", port))
		},
	}
	cmd.Flags().IntP("port", "p", 80, "port number to listen to")
	cmd.Flags().Duration("read-timeout", 30*time.Second, "request read timeout as described at https://pkg.go.dev/net/http#Server.ReadTimeout")
	cmd.Flags().Duration("write-timeout", 30*time.Second, "response write timeout as described at https://pkg.go.dev/net/http#Server.WriteTimeout")
	cmd.Flags().Bool("badger-log-info", false, "set Badger log level to INFO")
	cmd.Flags().Bool("badger-log-debug", false, "set Badger log level to DEBUG")
	cmd.AddCommand(newVersionCmd())
	return cmd
}
