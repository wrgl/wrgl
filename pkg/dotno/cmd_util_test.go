package dotno

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/conf"
)

type myObj struct {
	x int
}

func (o *myObj) String() string {
	return fmt.Sprintf("%d", o.x)
}

func TestMarshalText(t *testing.T) {
	for i, c := range []struct {
		Obj         interface{}
		ExpectedS   string
		ExpectedErr error
	}{
		{
			Obj:       "abc",
			ExpectedS: "abc",
		},
		{
			Obj:       uint64(123456),
			ExpectedS: "123456",
		},
		{
			Obj:       duration(12300000),
			ExpectedS: "12.3ms",
		},
		{
			Obj:       conf.FastForward("ff"),
			ExpectedS: "ff",
		},
		{
			Obj:       &myObj{123},
			ExpectedS: "123",
		},
		{
			Obj: &conf.AuthOIDCProvider{
				Issuer:   "http://oidc.google.com",
				ClientID: "123abc",
			},
			ExpectedS: `{"issuer":"http://oidc.google.com","clientID":"123abc"}`,
		},
	} {
		s, err := marshalText(reflect.ValueOf(c.Obj))
		if err != nil {
			assert.Equal(t, c.ExpectedErr, err, "case %d", i)
		} else {
			assert.Equal(t, c.ExpectedS, s)
		}
	}
}

func TestFilterWithValuePattern(t *testing.T) {
	for i, c := range []struct {
		Obj            interface{}
		Pattern        string
		FixedValue     bool
		ExpectedIdxMap map[int]struct{}
		ExpectedVals   []string
		ExpectedErr    error
	}{
		{
			Obj:         &conf.Auth{},
			Pattern:     "abc",
			ExpectedErr: errNotStringSlice,
		},
		{
			Obj:         []conf.AuthClient{},
			Pattern:     "abc",
			ExpectedErr: errNotStringSlice,
		},
		{
			Obj:         []*conf.Remote{},
			Pattern:     "abc",
			ExpectedErr: errNotStringSlice,
		},
		{
			Obj:            []string{"abc", "def"},
			Pattern:        "abc",
			ExpectedIdxMap: map[int]struct{}{0: {}},
			ExpectedVals:   []string{"abc"},
		},
		{
			Obj:            []string{"abc", "def"},
			Pattern:        "abc",
			FixedValue:     true,
			ExpectedIdxMap: map[int]struct{}{0: {}},
			ExpectedVals:   []string{"abc"},
		},
		{
			Obj:            []string{"abc", "def"},
			Pattern:        ".+f",
			ExpectedIdxMap: map[int]struct{}{1: {}},
			ExpectedVals:   []string{"def"},
		},
		{
			Obj:            []string{"abc", "def"},
			Pattern:        ".+f",
			FixedValue:     true,
			ExpectedIdxMap: map[int]struct{}{},
		},
		{
			Obj:         []string{"abc", "def"},
			Pattern:     "**",
			ExpectedErr: fmt.Errorf("error parsing VALUE_PATTERN: error parsing regexp: missing argument to repetition operator: `*`"),
		},
		{
			Obj: conf.RefspecSlice{
				conf.MustParseRefspec("refs/heads/main"),
				conf.MustParseRefspec("refs/remotes/origin/main"),
			},
			Pattern:        `^refs/heads/.+`,
			ExpectedIdxMap: map[int]struct{}{0: {}},
			ExpectedVals: []string{
				"refs/heads/main",
			},
		},
	} {
		cmd := &cobra.Command{Use: "filter"}
		cmd.Flags().Bool("fixed-value", false, "")
		if c.FixedValue {
			cmd.Flags().Set("fixed-value", "true")
		}
		m, sl, err := FilterWithValuePattern(cmd, reflect.ValueOf(c.Obj), c.Pattern)
		if err != nil {
			assert.Equal(t, c.ExpectedErr, err, "case %d", i)
		} else {
			assert.Equal(t, c.ExpectedIdxMap, m, "case %d", i)
			assert.Equal(t, c.ExpectedVals, sl, "case %d", i)
		}
	}
}

func TestOutputValues(t *testing.T) {
	for i, c := range []struct {
		Obj            interface{}
		Null           bool
		ExpectedOutput string
		ExpectedErr    error
	}{
		{
			Obj:            "abc",
			ExpectedOutput: "abc\n",
		},
		{
			Obj:            "abc",
			Null:           true,
			ExpectedOutput: "abc\x00",
		},
		{
			Obj:            []string{"abc", "def"},
			ExpectedOutput: "abc\ndef\n",
		},
		{
			Obj:            []string{"abc", "def"},
			Null:           true,
			ExpectedOutput: "abc\ndef\x00",
		},
	} {
		cmd := &cobra.Command{Use: "output"}
		cmd.Flags().Bool("null", false, "")
		out := bytes.NewBuffer(nil)
		cmd.SetOutput(out)
		if c.Null {
			cmd.Flags().Set("null", "true")
		}
		err := OutputValues(cmd, c.Obj)
		if err != nil {
			assert.Equal(t, c.ExpectedErr, err, "case %d", i)
		} else {
			assert.Equal(t, c.ExpectedOutput, out.String(), "case %d", i)
		}
	}
}
