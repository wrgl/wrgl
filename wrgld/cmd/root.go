package wrgld

import (
	_ "embed"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wrgl/wrgl/cmd/wrgl/utils"
	"github.com/wrgl/wrgl/pkg/conf"
	conffs "github.com/wrgl/wrgl/pkg/conf/fs"
	"github.com/wrgl/wrgl/pkg/local"
)

//go:embed VERSION
var version string

func init() {
	version = strings.TrimSpace(version)
}

func RootCmd() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "wrgld [WRGL_DIR]",
		Short: "Starts an HTTP server providing access to the repository at <working_dir>/.wrgl or WRGL_DIR folder if it is given.",
		Example: utils.CombineExamples([]utils.Example{
			{
				Comment: "starts HTTP API over <working_dir>/.wrgl at port 80",
				Line:    "wrgld",
			},
			{
				Comment: "starts HTTP API over directory my-repo and port 4000",
				Line:    "wrgld ./my-repo -p 4000",
			},
			{
				Comment: "increase read and write timeout",
				Line:    "wrgld --read-timeout 60s --write-timeout 60s",
			},
		}),
		Version: version,
		Args:    cobra.MaximumNArgs(1),
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
			port := viper.GetInt("port")
			readTimeout := viper.GetDuration("read-timeout")
			writeTimeout := viper.GetDuration("write-timeout")
			badgerLog := viper.GetString("badger-log")
			proxy := viper.GetString("proxy")
			init := viper.GetBool("init")
			configFrom := viper.GetString("init-config-from")
			rd := local.NewRepoDir(dir, badgerLog)
			defer rd.Close()
			if init && !rd.Exist() {
				cmd.Printf("initializing repo at %q\n", dir)
				var c *conf.Config
				if configFrom != "" {
					cs := conffs.NewStore(dir, conffs.FileSource, configFrom)
					c, err = cs.Open()
					if err != nil {
						return fmt.Errorf("error reading config from %q: %v", configFrom, err)
					}
					cmd.Printf("read initial config from %q\n", configFrom)
				}
				if err = rd.Init(); err != nil {
					return
				}
				if c != nil {
					cs := conffs.NewStore(dir, conffs.LocalSource, "")
					if err = cs.Save(c); err != nil {
						return
					}
				}
				cmd.Println("repo initialized")
			}
			var client *http.Client
			if proxy != "" {
				proxyURL, err := url.Parse(proxy)
				if err != nil {
					return err
				}
				transport := &http.Transport{}
				*transport = *(http.DefaultTransport).(*http.Transport)
				transport.Proxy = func(r *http.Request) (*url.URL, error) {
					return proxyURL, nil
				}
				client = &http.Client{
					Transport: transport,
				}
			}
			server, err := NewServer(rd, readTimeout, writeTimeout, client)
			if err != nil {
				return
			}
			defer server.Close()
			return server.Start(fmt.Sprintf(":%d", port))
		},
	}
	cmd.Flags().IntP("port", "p", 80, "port number to listen to")
	cmd.Flags().Duration("read-timeout", 30*time.Second, "request read timeout as described at https://pkg.go.dev/net/http#Server.ReadTimeout")
	cmd.Flags().Duration("write-timeout", 30*time.Second, "response write timeout as described at https://pkg.go.dev/net/http#Server.WriteTimeout")
	cmd.Flags().String("proxy", "", "make all outgoing requests through this proxy")
	cmd.Flags().String("badger-log", "", `set Badger log level, valid options are "error", "warning", "debug", and "info" (defaults to "error")`)
	cmd.Flags().Bool("init", false, "initialize repo at WRGL_DIR if not already initialized")
	cmd.Flags().String("init-config-from", "", "initialize repo with initial config from this location")
	viper.BindPFlags(cmd.Flags())
	viper.SetEnvPrefix("wrgld")
	viper.SetEnvKeyReplacer(strings.NewReplacer("-", "_"))
	viper.AutomaticEnv()
	return cmd
}
