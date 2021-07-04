// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package sorter

import (
	"fmt"
	"io"
	"time"

	"github.com/schollz/progressbar/v3"
)

func pbar(max int64, desc string, out io.Writer) *progressbar.ProgressBar {
	bar := progressbar.NewOptions64(
		max,
		progressbar.OptionSetDescription(desc),
		progressbar.OptionSetWriter(out),
		progressbar.OptionSetWidth(10),
		progressbar.OptionThrottle(65*time.Millisecond),
		progressbar.OptionShowCount(),
		progressbar.OptionShowIts(),
		progressbar.OptionOnCompletion(func() {
			fmt.Fprint(out, "\n")
		}),
		progressbar.OptionSpinnerType(14),
		progressbar.OptionFullWidth(),
	)
	bar.RenderBlank()
	return bar
}
