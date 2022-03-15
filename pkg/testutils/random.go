// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package testutils

import (
	"fmt"
	"math/rand"
)

const (
	lowerAlphaBytes = "abcdefghijklmnopqrstuvwxyz"
	letterBytes     = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ" // 62 possibilities
	letterIdxBits   = 6                                                                // 6 bits to represent 64 possibilities / indexes
	letterIdxMask   = 1<<letterIdxBits - 1                                             // All 1-bits, as many as letterIdxBits
)

func SecureRandomBytes(length int) []byte {
	var randomBytes = make([]byte, length)
	_, err := rand.Read(randomBytes)
	if err != nil {
		panic(err)
	}
	return randomBytes
}

func RandomSum() [16]byte {
	b := [16]byte{}
	_, err := rand.Read(b[:])
	if err != nil {
		panic(err)
	}
	return b
}

func brokenRandomString(length int, charSet string) string {
	result := make([]byte, length)
	bufferSize := int(float64(length) * 1.3)
	for i, j, randomBytes := 0, 0, []byte{}; i < length; j++ {
		if j%bufferSize == 0 {
			randomBytes = SecureRandomBytes(bufferSize)
		}
		if idx := int(randomBytes[j%length] & letterIdxMask); idx < len(charSet) {
			result[i] = charSet[idx]
			i++
		}
	}
	return string(result)
}

// BrokenRandomAlphaNumericString is broken
// so don't use it outside of tests
func BrokenRandomAlphaNumericString(length int) string {
	return brokenRandomString(length, letterBytes)
}

func BrokenRandomLowerAlphaString(length int) string {
	return brokenRandomString(length, lowerAlphaBytes)
}

func RandomEmail() string {
	return fmt.Sprintf("%s@%s.com",
		BrokenRandomLowerAlphaString(8),
		BrokenRandomLowerAlphaString(8),
	)
}
