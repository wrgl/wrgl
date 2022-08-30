package umasql

import (
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wrgl/wrgl/pkg/testutils"
)

func TestStore(t *testing.T) {
	db, cleanup := testutils.CreateSQLDB(t, CreateTableStmts)
	defer cleanup()
	s := NewStore(db)
	name := gofakeit.Name()
	id := gofakeit.UUID()
	require.NoError(t, s.Set(name, id))
	str, err := s.Get(name)
	require.NoError(t, err)
	assert.Equal(t, id, str)
}
