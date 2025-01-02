package db

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/x/x/logs"
	"github.com/x/x/modules"
	"github.com/surrealdb/surrealdb.go"
)

type SurrealInstance struct {
	DB       *surrealdb.DB
	closed   atomic.Bool
	b        time.Time
	conn     atomic.Int32
	lifetime time.Duration
	cond     *sync.Cond
	mu       sync.Mutex
}

type SurrealRootInstance struct {
	DB       *surrealdb.DB
	b        time.Time
	conn     atomic.Int32
	lifetime time.Duration
	cond     *sync.Cond
	mu       sync.Mutex
}

func (s *SurrealInstance) Close() error {
	if s.closed.Load() {
		logs.LogEvent("connection already closed", logs.DEBUG, "database/instance.go")
		return nil
	}

	timeout := time.After(3 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.conn.Load() > 0 {
		select {
		case <-timeout:
			logs.LogEvent("time out connection closed", logs.DEBUG, "database/instance.go")
			s.DB.Close()
			s.closed.Store(true)
			return modules.ErrTimeOut
		default:
			logs.LogEvent("waiting connection to close", logs.DEBUG, "database/instance.go")
			s.cond.Wait()
		}
	}

	logs.LogEvent("connection closed", logs.DEBUG, "database/instance.go")
	if err := s.DB.Close(); err != nil {
		return err
	}

	s.closed.Store(true)
	return nil
}

func (s *SurrealRootInstance) Shutdown() error {
	timeout := time.After(3 * time.Second)

	s.mu.Lock()
	defer s.mu.Unlock()

	for s.conn.Load() > 0 {
		select {
		case <-timeout:
			logs.LogEvent("time out connection shut down", logs.DEBUG, "database/instance.go")
			s.DB.Close()
			return modules.ErrTimeOut
		default:
			logs.LogEvent("waiting connection to shut down", logs.DEBUG, "database/instance.go")
			s.cond.Wait()
		}
	}

	logs.LogEvent("connection shut down", logs.DEBUG, "database/instance.go")
	if err := s.DB.Close(); err != nil {
		return err
	}

	return nil
}

func (s *SurrealInstance) ConnectionOut(reset bool) {
	if s.closed.Load() {
		logs.LogEvent("connection is already closed", logs.DEBUG, "database/instance.go")
		return
	}

	if reset {
		if err := s.DB.Invalidate(); err != nil {
			logs.LogEvent("error while connection reset", logs.DEBUG, "database/instance.go")
			return
		}
	}

	conn := s.conn.Load()
	if conn > 0 {
		s.conn.Store(conn - 1)
		logs.LogEvent(fmt.Sprintf("connection out, total connections %d", conn), logs.DEBUG, "database/instance.go")
		s.mu.Lock()
		s.cond.Signal()
		s.mu.Unlock()
	}
}

func (s *SurrealRootInstance) ConnectionOut() {
	conn := s.conn.Load()
	if conn > 0 {
		s.conn.Store(conn - 1)
		logs.LogEvent(fmt.Sprintf("connection out, total connections %d", conn), logs.DEBUG, "database/instance.go")
		s.mu.Lock()
		s.cond.Signal()
		s.mu.Unlock()
	}
}

func (s *SurrealInstance) HealthCheck() bool {
	if s == nil {
		return false
	}

	if s.closed.Load() || time.Since(s.b) > s.lifetime {
		logs.LogEvent("instance lifetime exceeded", logs.DEBUG, "database/instance.go")
		return false
	}

	query, err := surrealdb.Query[bool](s.DB, `RETURN true;`, map[string]interface{}{})
	if err != nil {
		logs.ReportError(err)
		return false
	}

	if query == nil {
		logs.LogEvent("health check query returned nil", logs.DEBUG, "database/instance.go")
		return false
	}

	if len(*query) == 0 {
		logs.LogEvent("health check query returned empty result", logs.DEBUG, "database/instance.go")
		return false
	}

	if !(*query)[0].Result {
		logs.LogEvent("health check query returned false", logs.DEBUG, "database/instance.go")
		return false
	}

	logs.LogEvent("health check success", logs.DEBUG, "database/instance.go")
	return true
}

func (s *SurrealRootInstance) HealthCheck() bool {
	if s == nil {
		return false
	}

	if time.Since(s.b) > s.lifetime {
		logs.LogEvent("instance lifetime exceeded", logs.DEBUG, "database/instance.go")
		return false
	}

	query, err := surrealdb.Query[bool](s.DB, `RETURN true;`, map[string]interface{}{})
	if err != nil {
		logs.ReportError(err)
		return false
	}

	if query == nil {
		logs.LogEvent("health check query returned nil", logs.DEBUG, "database/instance.go")
		return false
	}

	if len(*query) == 0 {
		logs.LogEvent("health check query returned empty result", logs.DEBUG, "database/instance.go")
		return false
	}

	if !(*query)[0].Result {
		logs.LogEvent("health check query returned false", logs.DEBUG, "database/instance.go")
		return false
	}

	logs.LogEvent("health check success", logs.DEBUG, "database/instance.go")
	return true
}

func createInstance(ctx context.Context) (*SurrealInstance, error) {
	databaseWs := os.Getenv("DATABASE_WEBSOCKET")
	url := fmt.Sprintf("%s/rpc", databaseWs)

	b := time.Now().UTC()
	db, err := surrealdb.New(url)
	if err != nil {
		return nil, err
	}
	db.WithContext(ctx)

	namespace := os.Getenv("NAMESPACE")
	database := os.Getenv("DATABASE")

	if err := db.Use(namespace, database); err != nil {
		return nil, err
	}

	inst := &SurrealInstance{
		DB:       db,
		lifetime: LifeTime,
		b:        b,
	}
	inst.cond = sync.NewCond(&inst.mu)

	if !inst.HealthCheck() {
		inst.Close()
		return nil, modules.ErrFailedDBConnection
	}

	return inst, nil
}

func createRootInstance(ctx context.Context) (*SurrealRootInstance, error) {
	databaseWs := os.Getenv("DATABASE_WEBSOCKET")
	url := fmt.Sprintf("%s/rpc", databaseWs)

	b := time.Now().UTC()
	db, err := surrealdb.New(url)
	if err != nil {
		return nil, err
	}

	namespace := os.Getenv("NAMESPACE")
	database := os.Getenv("DATABASE")
	username := os.Getenv("DATABASE_ROOT_USERNAME")
	password := os.Getenv("DATABASE_ROOT_PASSWORD")

	if err := db.Use(namespace, database); err != nil {
		return nil, err
	}

	if _, err := db.SignIn(&surrealdb.Auth{
		Username: username,
		Password: password,
	}); err != nil {
		return nil, err
	}
	db.WithContext(ctx)

	inst := &SurrealRootInstance{
		DB:       db,
		lifetime: RootLifeTime,
		b:        b,
	}
	inst.cond = sync.NewCond(&inst.mu)

	if !inst.HealthCheck() {
		inst.Shutdown()
		return nil, modules.ErrFailedDBConnection
	}

	return inst, nil
}
