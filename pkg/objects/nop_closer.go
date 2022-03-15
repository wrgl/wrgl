// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package objects

import "io"

type nopCloser struct {
	r io.ReadSeeker
}

func (n *nopCloser) Read(b []byte) (int, error) {
	return n.r.Read(b)
}

func (n *nopCloser) Seek(off int64, whence int) (int64, error) {
	return n.r.Seek(off, whence)
}

func (n *nopCloser) Close() error {
	return nil
}

func NopCloser(r io.ReadSeeker) io.ReadSeekCloser {
	return &nopCloser{r: r}
}
