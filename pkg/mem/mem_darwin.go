// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package mem

import (
	"os/exec"
	"strconv"
	"strings"
)

func GetTotalMem() (uint64, error) {
	out, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
	if err != nil {
		return 0, err
	}
	return strconv.ParseUint(strings.Trim(string(out), " \n"), 10, 64)
}

func GetAvailMem() (uint64, error) {
	out, err := exec.Command("bash", "-c", "vm_stat | sed '1d' | head -4 | awk '{ sum += $3 } END { print sum }'").Output()
	if err != nil {
		return 0, err
	}
	u, err := strconv.ParseUint(strings.Trim(string(out), " \n"), 10, 64)
	if err != nil {
		return 0, err
	}
	return u * 4096, nil
}
