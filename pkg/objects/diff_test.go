// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package objects

// func TestDiffWriter(t *testing.T) {
// 	objs := []*Diff{
// 		{Type: DTColumnChange, ColDiff: CompareColumns([2][]string{{"a", "b", "c"}, {"a"}}, [2][]string{{"a", "d", "b"}, {"a"}})},
// 		{Type: DTRow, PK: testutils.SecureRandomBytes(16), Sum: testutils.SecureRandomBytes(16)},
// 		{Type: DTRow, PK: testutils.SecureRandomBytes(16), OldSum: testutils.SecureRandomBytes(16)},
// 		{Type: DTRow, PK: testutils.SecureRandomBytes(16), OldSum: testutils.SecureRandomBytes(16), Sum: testutils.SecureRandomBytes(16)},
// 	}
// 	buf := bytes.NewBuffer(nil)
// 	w := NewDiffWriter(buf)
// 	for _, obj := range objs {
// 		require.NoError(t, w.Write(obj))
// 	}

// 	r := NewDiffReader(bytes.NewReader(buf.Bytes()))
// 	for _, obj := range objs {
// 		d, err := r.Read()
// 		require.NoError(t, err)
// 		assert.Equal(t, obj, d)
// 	}
// 	_, err := r.Read()
// 	assert.Equal(t, io.EOF, err)
// }
