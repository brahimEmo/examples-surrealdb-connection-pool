package db

import (
	"context"
	"regexp"
	"time"

	"github.com/x/x/modules"
	"github.com/surrealdb/surrealdb.go"
)

const DefaultTimeOut = 5 * time.Second

func SurrQL(sql string) string {
	re := regexp.MustCompile(`\s+`)
	return re.ReplaceAllString(sql, " ")
}

func Query[T any](ctx context.Context, sql string, vars map[string]interface{}) (T, error) {
	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	default:
	}

	pool := QueryPoolRegistry.GetPool()
	instance, err := pool.GetInstance()
	defer instance.ConnectionOut(true)
	if err != nil {
		var def T
		return def, err
	}

	query, err := surrealdb.Query[T](instance.DB, sql, vars)
	if err != nil {
		var def T
		return def, err
	}

	if query == nil || len(*query) == 0 {
		var def T
		return def, modules.ErrQueryFailed
	}

	return (*query)[0].Result, nil
}

func QueryWithTimeout[T any](ctx context.Context, timeout time.Duration, sql string, vars map[string]interface{}) (T, error) {
	ctx_, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	case <-ctx_.Done():
		var def T
		return def, modules.ErrTimeOut
	default:
	}

	return Query[T](ctx_, sql, vars)
}

func QueryWithAuth[T any](ctx context.Context, sql string, token string, vars map[string]interface{}) (T, error) {
	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	default:
	}

	pool := QueryPoolRegistry.GetPool()
	instance, err := pool.GetInstance()
	defer instance.ConnectionOut(true)
	if err != nil {
		var def T
		return def, err
	}

	db := instance.DB
	if err := db.Authenticate(token); err != nil {
		var def T
		return def, err
	}

	query, err := surrealdb.Query[T](db, sql, vars)
	if err != nil {
		var def T
		return def, err
	}

	if query == nil || len(*query) == 0 {
		var def T
		return def, modules.ErrQueryFailed
	}

	return (*query)[0].Result, nil
}

func QueryWithAuthTimeout[T any](ctx context.Context, timeout time.Duration, sql string, token string, vars map[string]interface{}) (T, error) {
	ctx_, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	case <-ctx_.Done():
		var def T
		return def, modules.ErrTimeOut
	default:
	}

	return QueryWithAuth[T](ctx_, sql, token, vars)
}

func RootQuery[T any](ctx context.Context, sql string, vars map[string]interface{}) (T, error) {
	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	default:
	}

	pool := RootMPoolRegistry.GetPool()
	instance, err := pool.GetInstance()
	defer instance.ConnectionOut()
	if err != nil {
		var def T
		return def, err
	}

	query, err := surrealdb.Query[T](instance.DB, sql, vars)
	if err != nil {
		var def T
		return def, err
	}

	if query == nil || len(*query) == 0 {
		var def T
		return def, modules.ErrQueryFailed
	}

	return (*query)[0].Result, nil
}

func RootQueryWithTimeout[T any](ctx context.Context, timeout time.Duration, sql string, vars map[string]interface{}) (T, error) {
	ctx_, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	select {
	case <-ctx.Done():
		var def T
		return def, ctx.Err()
	case <-ctx_.Done():
		var def T
		return def, modules.ErrTimeOut
	default:
	}

	return RootQuery[T](ctx_, sql, vars)
}
