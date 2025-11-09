package scheduler

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
)

// JobManager manages all background refresh jobs.
type JobManager struct {
	db         *sqlx.DB
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	jobMap     map[string]*jobEntry
	jobMapLock sync.Mutex
}

// jobEntry tracks an active table job.
type jobEntry struct {
	cancel   context.CancelFunc
	interval int
}

// NewJobManager initializes the job manager.
func NewJobManager(db *sqlx.DB) *JobManager {
	return &JobManager{
		db:     db,
		jobMap: make(map[string]*jobEntry),
	}
}

// Start launches the dynamic scheduler that checks tables periodically.
func (jm *JobManager) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	jm.cancel = cancel

	log.Println("[scheduler] Starting dynamic background refresh jobs...")

	ticker := time.NewTicker(30 * time.Second) // configurable interval
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jm.checkJobs(ctx)
		case <-ctx.Done():
			jm.stopAllJobs()
			log.Println("[scheduler] Scheduler stopped gracefully")
			return
		}
	}
}

// checkJobs inspects DB and starts/stops jobs as needed.
func (jm *JobManager) checkJobs(parentCtx context.Context) {
	var tables []struct {
		TableName       string `db:"table_name"`
		RefreshInterval int    `db:"refresh_interval"`
	}

	err := jm.db.Select(&tables, `
		SELECT table_name, refresh_interval 
		FROM table_metadata 
		WHERE table_type = 'time_series' AND refresh_interval IS NOT NULL
	`)
	if err != nil {
		log.Printf("[scheduler] Failed to fetch tables: %v", err)
		return
	}

	jm.jobMapLock.Lock()
	defer jm.jobMapLock.Unlock()

	currentTables := make(map[string]bool)

	for _, t := range tables {
		currentTables[t.TableName] = true
		entry, exists := jm.jobMap[t.TableName]

		if !exists {
			jm.startJob(parentCtx, t.TableName, t.RefreshInterval)
		} else if entry.interval != t.RefreshInterval {
			log.Printf("[scheduler] Updating interval for table %s: %d -> %d", t.TableName, entry.interval, t.RefreshInterval)
			entry.cancel()
			jm.startJob(parentCtx, t.TableName, t.RefreshInterval)
		}
	}

	// Stop jobs for tables no longer present
	for tableName, entry := range jm.jobMap {
		if !currentTables[tableName] {
			log.Printf("[scheduler] Table %s removed, stopping job", tableName)
			entry.cancel()
			delete(jm.jobMap, tableName)
		}
	}
}

// startJob launches a goroutine to refresh a single table.
func (jm *JobManager) startJob(parentCtx context.Context, tableName string, interval int) {
	jobCtx, cancel := context.WithCancel(parentCtx)

	jm.jobMapLock.Lock()
	jm.jobMap[tableName] = &jobEntry{cancel: cancel, interval: interval}
	jm.jobMapLock.Unlock()

	jm.wg.Add(1)
	go func() {
		defer jm.wg.Done()
		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()
		log.Printf("[scheduler] Started auto-refresh for table %s (every %d sec)", tableName, interval)

		for {
			select {
			case <-ticker.C:
				if err := jm.refreshTable(tableName); err != nil {
					log.Printf("[scheduler] Error refreshing %s: %v", tableName, err)
				} else {
					log.Printf("[scheduler] Table %s refreshed successfully", tableName)
				}
			case <-jobCtx.Done():
				log.Printf("[scheduler] Stopping refresh job for table: %s", tableName)
				return
			}
		}
	}()
}

// refreshTable contains the actual refresh logic for a table.
func (jm *JobManager) refreshTable(tableName string) error {

	// TO DO : Add the actual refresh logic needed
	_, err := jm.db.Exec(`UPDATE table_metadata SET updated_at = NOW() WHERE table_name = $1`, tableName)
	return err
}

// stopAllJobs stops all active jobs
func (jm *JobManager) stopAllJobs() {
	jm.jobMapLock.Lock()
	defer jm.jobMapLock.Unlock()

	for tableName, entry := range jm.jobMap {
		log.Printf("[scheduler] Stopping job for table: %s", tableName)
		entry.cancel()
		delete(jm.jobMap, tableName)
	}

	jm.wg.Wait()
}

// Stop gracefully stops all jobs and the scheduler
func (jm *JobManager) Stop() {
	if jm.cancel != nil {
		log.Println("[scheduler] Stopping all refresh jobs...")
		jm.cancel()
		jm.wg.Wait()
		log.Println("[scheduler] All jobs stopped cleanly")
	}
}
