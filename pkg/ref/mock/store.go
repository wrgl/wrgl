// SPDX-License-Identifier: Apache-2.0
// Copyright © 2022 Wrangle Ltd

package refmock

import (
	"database/sql"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/ref"
	refsql "github.com/wrgl/wrgl/pkg/ref/sql"
	"github.com/wrgl/wrgl/pkg/sqlutil"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func NewStore(t *testing.T) (ref.Store, func()) {
	t.Helper()
	db, err := sql.Open(
		"sqlite3",
		fmt.Sprintf("file:%x.db?cache=shared&mode=memory", testutils.SecureRandomBytes(4)),
	)
	require.NoError(t, err)
	require.NoError(t, sqlutil.RunInTx(db, func(tx *sql.Tx) error {
		for _, stmt := range refsql.CreateTableStmts {
			if _, err := tx.Exec(stmt); err != nil {
				return err
			}
		}
		return nil
	}))
	return refsql.NewStore(db), func() {
		require.NoError(t, db.Close())
	}
}
