// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package ingest

func getTotalMem() (uint64, error) {
	out, err := exec.Command("awk", "/MemTotal/ {print $2}", "/proc/meminfo")
	if err != nil {
		return 0, err
	}
	u, err := strconv.ParseUint(strings.Trim(string(out), " \n"), 10, 64)
	if err != nil {
		return 0, err
	}
	return u * 1024, nil
}

func getAvailMem() (uint64, error) {
	out, err := exec.Command("awk", "/MemFree/ {print $2}", "/proc/meminfo")
	if err != nil {
		return 0, err
	}
	u, err := strconv.ParseUint(strings.Trim(string(out), " \n"), 10, 64)
	if err != nil {
		return 0, err
	}
	return u * 1024, nil
}
