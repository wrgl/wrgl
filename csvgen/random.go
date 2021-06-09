// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package main

import "crypto/rand"

const (
	lowerAlphaBytes = "abcdefghijklmnopqrstuvwxyz"
	letterBytes     = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // 62 possibilities
	letterIdxBits   = 6                                                                // 6 bits to represent 64 possibilities / indexes
	letterIdxMask   = 1<<letterIdxBits - 1                                             // All 1-bits, as many as letterIdxBits
)

func secureRandomBytes(length int) []byte {
	var randomBytes = make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	return randomBytes
}

func brokenRandomString(length int, charSet string) string {
	result := make([]byte, length)
	bufferSize := int(float64(length) * 1.3)
	for i, j, randomBytes := 0, 0, []byte{}; i < length; j++ {
		if j%bufferSize == 0 {
			randomBytes = secureRandomBytes(bufferSize)
		}
		if idx := int(randomBytes[j%length] & letterIdxMask); idx < len(charSet) {
			result[i] = charSet[idx]
			i++
		}
	}
	return string(result)
}

func brokenRandomAlphaNumericString(length int) string {
	return brokenRandomString(length, letterBytes)
}

func brokenRandomLowerAlphaString(length int) string {
	return brokenRandomString(length, lowerAlphaBytes)
}
