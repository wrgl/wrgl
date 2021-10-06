// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package progress

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestTracker(t *testing.T) {
	tr := NewSingleTracker(10*time.Millisecond, 10)
	c := tr.Start()
	tr.SetCurrent(1)
	testutils.Retry(t, 10*time.Millisecond, 10, func() bool {
		return assert.ObjectsAreEqual(Event{
			Progress: 1,
			Total:    10,
		}, <-c)
	}, "")

	tr.Add(2)
	testutils.Retry(t, 10*time.Millisecond, 10, func() bool {
		return assert.ObjectsAreEqual(Event{
			Progress: 3,
			Total:    10,
		}, <-c)
	}, "")

	tr.Stop()
	ok := true
	for ok {
		_, ok = <-c
	}
	// c is closed after a few straggler events
}

func TestJoinChannels(t *testing.T) {
	tr1 := NewSingleTracker(3*time.Millisecond, 10)
	tr2 := NewSingleTracker(3*time.Millisecond, 15)
	tr3 := NewSingleTracker(3*time.Millisecond, 27)
	tr := JoinTrackers(tr1, tr2, tr3)
	assert.Equal(t, 3*time.Millisecond, tr.Duration())
	c := tr.Start()

	tr1.SetCurrent(3)
	for e := range c {
		assert.Equal(t, int64(52), e.Total)
		if e.Progress == 3 {
			break
		}
	}

	tr2.SetCurrent(2)
	for e := range c {
		assert.Equal(t, int64(52), e.Total)
		if e.Progress == 5 {
			break
		}
	}

	tr3.SetCurrent(12)
	for e := range c {
		assert.Equal(t, int64(52), e.Total)
		if e.Progress == 17 {
			break
		}
	}

	tr.Stop()
	ok := true
	for ok {
		_, ok = <-c
	}
}
