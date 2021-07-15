// SPDX-License-Identifier: Apache-2.0
// Copyright © 2021 Wrangle Ltd

package api

// func TestNegotiatorFoundUnrecognizedWants(t *testing.T) {
// 	db := objmock.NewStore()
// 	rs := refmock.NewStore()
// 	sum1, _ := refhelpers.SaveTestCommit(t, db, nil)
// 	sum2, c2 := refhelpers.SaveTestCommit(t, db, [][]byte{sum1})
// 	require.NoError(t, ref.CommitHead(rs, "main", sum2, c2))
// 	sum3 := testutils.SecureRandomBytes(16)
// 	ses := NewUploadPackSession(db, rs, "/upload-pack/", "test")
// 	_, err := ses.ServeHTTP(db, rs, nil, [][]byte{sum1}, false)
// 	assert.Error(t, err, "empty wants list")
// 	_, err = ses.ServeHTTP(db, rs, [][]byte{sum3}, [][]byte{sum1}, false)
// 	assert.Error(t, err, "unrecognized wants: "+hex.EncodeToString(sum3))
// }
