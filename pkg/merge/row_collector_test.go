package merge

// func TestRowCollector(t *testing.T) {
// 	db := kv.NewMockStore(false)
// 	fs := kv.NewMockStore(false)
// 	baseSum, baseCom := factory.Commit(t, db, fs, []string{
// 		"a,b,c",
// 		"1,q,w",
// 		"2,a,s",
// 	}, []uint32{0}, nil)
// 	baseT, err := table.ReadTable(db, fs, baseCom.Table)
// 	require.NoError(t, err)
// 	sum1, com1 := factory.Commit(t, db, fs, []string{
// 		"a,b,c",
// 		"1,q,r",
// 		"2,a,s",
// 	}, []uint32{0}, [][]byte{baseSum})
// 	sum2, com2 := factory.Commit(t, db, fs, []string{
// 		"a,b,c",
// 		"1,e,w",
// 		"3,s,d",
// 	}, []uint32{0}, [][]byte{baseSum})
// 	colDiff := objects.CompareColumns([2][]string{baseT.Columns(), baseT.PrimaryKey()}, cols...)
// }
