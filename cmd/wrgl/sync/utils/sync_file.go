package utils

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
	"gopkg.in/yaml.v3"
)

type Repo struct {
	FileName   string   `yaml:"fileName,omitempty"`
	PrimaryKey []string `yaml:"primaryKey,omitempty"`
	URI        string   `yaml:"uri"`
}

type MessageContextGitCommit struct {
	Link    string `yaml:"link,omitempty"`
	Message string `yaml:"message,omitempty"`
}

type MessageContext struct {
	GitCommit *MessageContextGitCommit `yaml:"gitCommit,omitempty"`
}

type GitRemote struct {
	Host string `yaml:"host,omitempty"`
	Dir  string `yaml:"dir,omitempty"`
}

type CommitTemplate struct {
	MessageTemplate string     `yaml:"messageTemplate,omitempty"`
	GitRemote       *GitRemote `yaml:"gitRemote,omitempty"`
	GitRoot         string     `yaml:"gitRoot,omitempty"`
}

type SyncFile struct {
	Repos          map[string]Repo `yaml:"repos,omitempty"`
	CommitTemplate *CommitTemplate `yaml:"commitTemplate,omitempty"`
}

func getSyncFilePath(cmd *cobra.Command) (fp string, err error) {
	dir, err := cmd.Flags().GetString("dir")
	if err != nil {
		return
	}
	if dir == "" {
		dir, err = os.Getwd()
		if err != nil {
			return "", err
		}
	}
	for _, fn := range []string{".wrglsync.yaml", ".wrglsync.yml"} {
		fp = filepath.Join(dir, fn)
		if _, err = os.Stat(fp); err == os.ErrNotExist {
			continue
		} else if err != nil {
			return "", err
		}
		return fp, nil
	}
	return "", fmt.Errorf("sync file does not exist in folder %s", dir)
}

func ReadSyncFile(cmd *cobra.Command) (sf *SyncFile, err error) {
	fp, err := getSyncFilePath(cmd)
	if err != nil {
		return
	}
	f, err := os.Open(fp)
	if err != nil {
		return
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return
	}
	if err = yaml.Unmarshal(b, sf); err != nil {
		return nil, err
	}
	return sf, nil
}

func MustReadSyncFile(cmd *cobra.Command) (sf *SyncFile) {
	sf, err := ReadSyncFile(cmd)
	if err != nil {
		cmd.PrintErrln(err.Error())
		os.Exit(1)
	}
	return
}

func (sf *SyncFile) Save(cmd *cobra.Command) (err error) {
	fp, err := getSyncFilePath(cmd)
	if err != nil {
		return
	}
	b, err := yaml.Marshal(sf)
	if err != nil {
		return
	}
	return ioutil.WriteFile(fp, b, 0644)
}
