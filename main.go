package db

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/x/x/logs"
	"github.com/x/x/modules"
	"github.com/x/x/utils"
	"github.com/jedib0t/go-pretty/v6/table"
)

const MaxConnections int32 = 24
const LifeTime time.Duration = time.Hour
const RootLifeTime time.Duration = time.Minute * 30

func (pool *SurrealPool) GetInstance() (*SurrealInstance, error) {
	select {
	case <-pool.ctx.Done():
		return nil, modules.ErrServerShuttingDown
	default:
	}

	pool.mu.RLock()
	copy := pool.instances
	pool.mu.RUnlock()
	validInstances := make([]*SurrealInstance, 0, len(copy))
	for _, instance := range copy {
		valid := instance != nil && !instance.closed.Load() && time.Since(instance.b) <= instance.lifetime

		if valid {
			validInstances = append(validInstances, instance)
		}
	}

	if len(validInstances) == 0 {
		ctx, cancel := context.WithTimeout(pool.ctx, 10*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			return nil, modules.ErrTimeOut
		default:
		}

		logs.LogEvent("fallback", logs.WARN, "database/main.go")
		fallbackPool := QueryPoolRegistry.GetPool()
		return fallbackPool.GetInstance()
	}

	idx := pool.idx.Add(1) % int32(len(validInstances))
	selected := validInstances[idx]

	selected.mu.Lock()
	defer selected.mu.Unlock()

	conn := selected.conn.Load()
	for conn >= MaxConnections {
		selected.cond.Wait()
	}

	selected.conn.Add(1)
	return selected, nil
}

func (pool *SurrealRootPool) GetInstance() (*SurrealRootInstance, error) {
	select {
	case <-pool.ctx.Done():
		return nil, modules.ErrServerShuttingDown
	default:
	}

	pool.mu.RLock()
	copy := pool.instances
	pool.mu.RUnlock()
	validInstances := make([]*SurrealRootInstance, 0, len(copy))
	for _, instance := range copy {
		valid := instance != nil && time.Since(instance.b) <= instance.lifetime

		if valid {
			validInstances = append(validInstances, instance)
		}
	}

	if len(validInstances) == 0 {
		ctx, cancel := context.WithTimeout(pool.ctx, 10*time.Second)
		defer cancel()

		select {
		case <-ctx.Done():
			return nil, modules.ErrTimeOut
		default:
		}

		logs.LogEvent("fallback", logs.WARN, "database/main.go")
		fallbackPool := RootMPoolRegistry.GetPool()
		return fallbackPool.GetInstance()
	}

	idx := pool.idx.Add(1) % int32(len(validInstances))
	selected := validInstances[idx]

	selected.mu.Lock()
	defer selected.mu.Unlock()

	conn := selected.conn.Load()
	for conn >= MaxConnections {
		selected.cond.Wait()
	}

	selected.conn.Add(1)
	return selected, nil
}

func (pool *SurrealPool) CloseAll() {
	var wg sync.WaitGroup

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for i, instance := range pool.instances {
		wg.Add(1)
		go func(inst *SurrealInstance, idx int) {
			defer wg.Done()
			if err := inst.Close(); err != nil {
				logs.ReportError(err)
			}
		}(instance, i)
	}

	wg.Wait()
}

// !!! caution
func (pool *SurrealRootPool) CloseAll() {
	var wg sync.WaitGroup

	pool.mu.Lock()
	defer pool.mu.Unlock()

	for i, instance := range pool.instances {
		wg.Add(1)
		go func(inst *SurrealRootInstance, idx int) {
			defer wg.Done()
			if err := inst.Shutdown(); err != nil {
				logs.ReportError(err)
			}
		}(instance, i)
	}

	wg.Wait()
}

type PoolRegistry struct {
	pools []*SurrealPool

	mu  sync.RWMutex
	idx atomic.Int32
}

type RootPoolRegistry struct {
	pools []*SurrealRootPool

	mu  sync.RWMutex
	idx atomic.Int32
}

func (r *PoolRegistry) GetPool() *SurrealPool {
	r.mu.RLock()
	copy := r.pools
	r.mu.RUnlock()

	if len(copy) == 0 {
		logs.LogEvent("empty pool registry", logs.DEBUG, "database/main.go")
		panic(modules.ErrFailedToInitialize)
	}

	idx := r.idx.Add(1) - 1
	if idx >= int32(len(copy)) {
		idx = 0
		r.idx.Store(1)
	}
	selected := copy[idx]

	logs.LogEvent("pool selected", logs.DEBUG, "database/main.go")
	return selected
}

func (r *PoolRegistry) Status() []table.Row {
	r.mu.RLock()
	copy := r.pools
	r.mu.RUnlock()
	rows := make([]table.Row, 0)
	for idx, pool := range copy {
		pool.mu.RLock()
		icopy := pool.instances
		pool.mu.RUnlock()
		available := len(icopy)
		conns := int32(0)
		for _, instance := range icopy {
			conns += instance.conn.Load()
		}
		status := "ok"
		ok := pool.ok.Load()
		if !ok {
			status = "error"
		}
		rows = append(rows, table.Row{"Main", fmt.Sprintf("pool_%d", idx+1), available, conns, status})
	}
	return rows
}

func (r *RootPoolRegistry) GetPool() *SurrealRootPool {
	r.mu.RLock()
	copy := r.pools
	r.mu.RUnlock()

	if len(copy) == 0 {
		logs.LogEvent("empty pool registry", logs.DEBUG, "database/main.go")
		panic(modules.ErrFailedToInitialize)
	}

	idx := r.idx.Add(1) - 1
	if idx >= int32(len(copy)) {
		idx = 0
		r.idx.Store(1)
	}
	selected := copy[idx]

	logs.LogEvent("pool selected", logs.DEBUG, "database/main.go")
	return selected
}

func (r *RootPoolRegistry) Status() []table.Row {
	r.mu.RLock()
	copy := r.pools
	r.mu.RUnlock()
	rows := make([]table.Row, 0)
	for idx, pool := range copy {
		pool.mu.RLock()
		icopy := pool.instances
		pool.mu.RUnlock()
		available := len(icopy)
		conns := int32(0)
		for _, instance := range icopy {
			conns += instance.conn.Load()
		}
		status := "ok"
		ok := pool.ok.Load()
		if !ok {
			status = "error"
		}
		rows = append(rows, table.Row{"Root", fmt.Sprintf("pool_%d", idx+1), available, conns, status})
	}
	return rows
}

var (
	QueryPoolRegistry *PoolRegistry
	cleanupMprOnce    sync.Once
)

var (
	RootMPoolRegistry *RootPoolRegistry
	cleanupRprOnce    sync.Once
)

var (
	managerOnce    sync.Once
	managerOnceErr error
)

func Initialize(ctx context.Context) error {
	numPools := utils.GetEnvAsInt("POOL_PER_REGISTRY", 2)
	limitPerPool := utils.GetEnvAsInt("POOL_LIMIT", 3)
	startPerPool := utils.GetEnvAsInt("POOL_START", 1)

	managerOnce.Do(func() {
		t := time.Now()
		QueryPoolRegistry = &PoolRegistry{
			pools: func() []*SurrealPool {
				pools := make([]*SurrealPool, numPools)
				for i := 0; i < numPools; i++ {
					pool, err := NewSurrealPool(limitPerPool, startPerPool, ctx)
					if err != nil {
						managerOnceErr = err
						return nil
					}
					pools[i] = pool
				}

				return pools
			}(),
		}

		RootMPoolRegistry = &RootPoolRegistry{
			pools: func() []*SurrealRootPool {
				pools := make([]*SurrealRootPool, numPools)
				for i := 0; i < numPools; i++ {
					pool, err := NewSurrealRootPool(limitPerPool, startPerPool, ctx)
					if err != nil {
						managerOnceErr = err
						return nil
					}
					pools[i] = pool
				}

				return pools
			}(),
		}
		logs.LogEvent(fmt.Sprintf("proccess took %.2f", time.Since(t).Seconds()), logs.DEBUG, "database/main.go")
	})

	return managerOnceErr
}

func (m *PoolRegistry) Cleanup() {
	cleanupMprOnce.Do(func() {
		for _, pool := range m.pools {
			pool.CloseAll()
		}
	})
}

func (m *RootPoolRegistry) Cleanup() {
	cleanupRprOnce.Do(func() {
		for _, pool := range m.pools {
			pool.CloseAll()
		}
	})
}
