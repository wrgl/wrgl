// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package conf

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestConfigRemoteFetchDstMatchRef(t *testing.T) {
	cr := &ConfigRemote{
		Fetch: []*Refspec{
			MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
			MustParseRefspec("refs/tags/v1.0.0:refs/tags/v1.0.0"),
		},
	}
	assert.True(t, cr.FetchDstMatchRef("refs/remotes/origin/abc"))
	assert.True(t, cr.FetchDstMatchRef("refs/tags/v1.0.0"))
	assert.False(t, cr.FetchDstMatchRef("refs/tags/v2.1.0"))
}

func TestConfigRemoteFetchDstForRef(t *testing.T) {
	cr := &ConfigRemote{
		Fetch: []*Refspec{
			MustParseRefspec("+refs/heads/*:refs/remotes/origin/*"),
			MustParseRefspec("refs/tags/v1.0.0:refs/tags/v1.0.0"),
			MustParseRefspec("^refs/heads/nah"),
		},
	}
	assert.Equal(t, cr.FetchDstForRef("refs/heads/abc"), "refs/remotes/origin/abc")
	assert.Equal(t, cr.FetchDstForRef("refs/heads/nah"), "")
	assert.Equal(t, cr.FetchDstForRef("refs/tags/v1.0.0"), "refs/tags/v1.0.0")
	assert.Equal(t, cr.FetchDstForRef("refs/tags/v1.1.0"), "")
}
