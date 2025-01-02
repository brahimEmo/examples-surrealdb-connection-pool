package db_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/x/x/db"
)

func TestMain(m *testing.M) {
	env := os.Getenv("GO_ENVIRONMENT")

	if env != "development" {
		os.Exit(1)
	}

	code := m.Run()
	os.Exit(code)
}

func TestDatabase(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := time.Now().UTC()
	err := db.Initialize(ctx)
	if err != nil {
		t.Fatal(err)
		return
	}

	_, err = db.RootQueryWithTimeout[bool](ctx, db.DefaultTimeOut, `RETURN true`, map[string]interface{}{})
	if err != nil {
		t.Fatal(err)
		return
	}

	td := time.Since(st)
	t.Logf("test took %.2f seconds", td.Seconds())

	t.Cleanup(func() {
		t.Log("cleaning up")
		db.QueryPoolRegistry.Cleanup()
		db.RootMPoolRegistry.Cleanup()
	})
}
