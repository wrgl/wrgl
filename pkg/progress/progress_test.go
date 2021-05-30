// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package progress

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestTracker(t *testing.T) {
	tr := NewTracker(10*time.Millisecond, 10)
	go tr.Run()

	tr.SetCurrent(1)
	c := tr.Chan()
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, Event{
		Progress: 1,
		Total:    10,
	}, <-c)

	tr.SetCurrent(3)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, Event{
		Progress: 3,
		Total:    10,
	}, <-c)

	tr.Stop()
	_, ok := <-c
	assert.False(t, ok)
}

func TestJoinChannels(t *testing.T) {
	tr1 := NewTracker(3*time.Millisecond, 10)
	tr2 := NewTracker(3*time.Millisecond, 15)
	tr3 := NewTracker(3*time.Millisecond, 27)
	tr := JoinTrackers(tr1, tr2, tr3)
	assert.Equal(t, 3*time.Millisecond, tr.Duration())
	c := tr.Chan()
	go tr.Run()

	tr1.SetCurrent(3)
	time.Sleep(3 * time.Millisecond)
	assert.Equal(t, Event{
		Progress: 3,
		Total:    52,
	}, <-c)

	tr2.SetCurrent(2)
	time.Sleep(3 * time.Millisecond)
	assert.Equal(t, Event{
		Progress: 5,
		Total:    52,
	}, <-c)

	tr3.SetCurrent(12)
	time.Sleep(3 * time.Millisecond)
	assert.Equal(t, Event{
		Progress: 17,
		Total:    52,
	}, <-c)

	tr.Stop()
	_, ok := <-c
	assert.False(t, ok)
}
