package db

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/x/x/logs"
	"github.com/x/x/utils"
)

type SurrealPool struct {
	instances []*SurrealInstance
	limit     int
	ok        atomic.Bool
	ctx       context.Context
	mu        sync.RWMutex
	idx       atomic.Int32
}

type SurrealRootPool struct {
	instances []*SurrealRootInstance
	limit     int
	ok        atomic.Bool
	ctx       context.Context
	mu        sync.RWMutex
	idx       atomic.Int32
}

func NewSurrealPool(limit int, start int, ctx context.Context) (*SurrealPool, error) {
	pool := &SurrealPool{
		limit: limit,
		ctx:   ctx,
	}

	for i := 0; i < start; i++ {
		instance, err := createInstance(ctx)
		if err != nil {
			return nil, err
		}

		pool.instances = append(pool.instances, instance)
	}

	go pool.MonitorHealth()
	pool.ok.Store(true)

	logs.LogEvent("new pool", logs.DEBUG, "database/pool.go")
	return pool, nil
}

func NewSurrealRootPool(limit int, start int, ctx context.Context) (*SurrealRootPool, error) {
	pool := &SurrealRootPool{
		limit: limit,
		ctx:   ctx,
	}

	for i := 0; i < start; i++ {
		instance, err := createRootInstance(ctx)
		if err != nil {
			return nil, err
		}

		pool.instances = append(pool.instances, instance)
	}

	go pool.MonitorHealth()
	pool.ok.Store(true)

	logs.LogEvent("new pool", logs.DEBUG, "database/pool.go")
	return pool, nil
}

func (pool *SurrealPool) MonitorHealth() {
	start := utils.GetEnvAsInt("POOL_START", 1)

	for {
		time.Sleep(LifeTime)

		pool.mu.RLock()
		instances := pool.instances
		pool.mu.RUnlock()

		excess := len(instances) - start
		healthyInstances := make([]*SurrealInstance, 0, len(instances))

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, instance := range instances {
			wg.Add(1)
			go func(inst *SurrealInstance) {
				defer wg.Done()

				if inst.HealthCheck() {
					mu.Lock()
					healthyInstances = append(healthyInstances, inst)
					mu.Unlock()
				} else {
					logs.LogEvent("health monitoring failed, creating new instance", logs.DEBUG, "database/pool.go")
					newInst, err := createInstance(pool.ctx)
					if err != nil {
						logs.ReportError(err)
					} else {
						mu.Lock()
						healthyInstances = append(healthyInstances, newInst)
						mu.Unlock()
					}
				}
			}(instance)
		}
		wg.Wait()

		if excess > 0 {
			closeWg := sync.WaitGroup{}
			for i := 0; i < excess; i++ {
				closeWg.Add(1)
				go func(inst *SurrealInstance) {
					defer closeWg.Done()
					if err := inst.Close(); err != nil {
						logs.ReportError(err)
					}
				}(healthyInstances[i])
			}
			closeWg.Wait()
			healthyInstances = healthyInstances[excess:]
		}

		for len(healthyInstances) < start {
			newInst, err := createInstance(pool.ctx)
			if err != nil {
				logs.ReportError(err)
				break
			}
			healthyInstances = append(healthyInstances, newInst)
		}

		pool.mu.Lock()
		pool.instances = healthyInstances
		pool.mu.Unlock()

		pool.ok.Store(len(healthyInstances) >= start)
	}
}

func (pool *SurrealRootPool) MonitorHealth() error {
	start := utils.GetEnvAsInt("POOL_START", 1)

	for {
		time.Sleep(RootLifeTime)

		pool.mu.RLock()
		instances := pool.instances
		pool.mu.RUnlock()

		excess := len(instances) - start
		healthyInstances := make([]*SurrealRootInstance, 0, len(instances))

		wg := sync.WaitGroup{}
		mu := sync.Mutex{}
		for _, instance := range instances {
			wg.Add(1)
			go func(inst *SurrealRootInstance) {
				defer wg.Done()

				if inst.HealthCheck() {
					mu.Lock()
					healthyInstances = append(healthyInstances, inst)
					mu.Unlock()
				} else {
					logs.LogEvent("health monitoring failed, creating new instance", logs.DEBUG, "database/pool.go")
					newInst, err := createRootInstance(pool.ctx)
					if err != nil {
						logs.ReportError(err)
					} else {
						mu.Lock()
						healthyInstances = append(healthyInstances, newInst)
						mu.Unlock()
					}
				}
			}(instance)
		}
		wg.Wait()

		if excess > 0 {
			closeWg := sync.WaitGroup{}
			for i := 0; i < excess; i++ {
				closeWg.Add(1)
				go func(inst *SurrealRootInstance) {
					defer closeWg.Done()
					if err := inst.Shutdown(); err != nil {
						logs.ReportError(err)
					}
				}(healthyInstances[i])
			}
			closeWg.Wait()
			healthyInstances = healthyInstances[excess:]
		}

		for len(healthyInstances) < start {
			newInst, err := createRootInstance(pool.ctx)
			if err != nil {
				logs.ReportError(err)
				break
			}
			healthyInstances = append(healthyInstances, newInst)
		}

		pool.mu.Lock()
		pool.instances = healthyInstances
		pool.mu.Unlock()

		pool.ok.Store(len(healthyInstances) >= start)
	}
}
