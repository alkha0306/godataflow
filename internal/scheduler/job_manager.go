package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/alkha0306/godataflow/internal/etl"
	"github.com/jmoiron/sqlx"
)

// -----------------------------------------------------
// JobManager orchestrates ETL jobs for time-series tables
// -----------------------------------------------------
type JobManager struct {
	db         *sqlx.DB
	etl        *etl.ETLProcessor
	wg         sync.WaitGroup
	cancel     context.CancelFunc
	started    bool
	jobMap     map[string]*jobEntry
	jobMapLock sync.Mutex
}

type jobEntry struct {
	cancel   context.CancelFunc
	interval int
}

// -----------------------------------------------------
// Constructor
// -----------------------------------------------------
func NewJobManager(db *sqlx.DB) *JobManager {
	return &JobManager{
		db:     db,
		etl:    etl.NewETLProcessor(db),
		jobMap: make(map[string]*jobEntry),
	}
}

// -----------------------------------------------------
// Start: Scheduler Loop
// Checks metadata periodically and launches/updates jobs
// -----------------------------------------------------
func (jm *JobManager) Start(ctx context.Context) {
	if jm.started {
		log.Println("[scheduler] JobManager already running")
		return
	}
	jm.started = true

	ctx, cancel := context.WithCancel(ctx)
	jm.cancel = cancel

	log.Println("[scheduler] Starting auto-refresh scheduler...")

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			jm.checkJobs(ctx)
		case <-ctx.Done():
			jm.stopAllJobs()
			log.Println("[scheduler] Scheduler stopped gracefully.")
			return
		}
	}
}

// -----------------------------------------------------
// checkJobs: Detects new, changed, or removed table jobs
// -----------------------------------------------------
func (jm *JobManager) checkJobs(parentCtx context.Context) {
	var tables []struct {
		TableName       string  `db:"table_name"`
		RefreshInterval int     `db:"refresh_interval"`
		DataSourceURL   *string `db:"data_source_url"`
	}

	err := jm.db.Select(&tables, `
		SELECT table_name, refresh_interval, data_source_url
		FROM table_metadata
		WHERE table_type = 'time_series'
		AND refresh_interval IS NOT NULL
		AND data_source_url IS NOT NULL;
	`)
	if err != nil {
		log.Printf("[scheduler] Error loading tables: %v", err)
		return
	}

	jm.jobMapLock.Lock()
	defer jm.jobMapLock.Unlock()

	currentTables := map[string]bool{}

	for _, t := range tables {
		currentTables[t.TableName] = true
		entry, running := jm.jobMap[t.TableName]

		// Start new job
		if !running {
			jm.startJob(parentCtx, t.TableName, t.RefreshInterval)
			continue
		}

		// Update interval
		if entry.interval != t.RefreshInterval {
			log.Printf("[scheduler] Interval update for %s (%d → %d)", t.TableName, entry.interval, t.RefreshInterval)
			entry.cancel()
			jm.startJob(parentCtx, t.TableName, t.RefreshInterval)
		}
	}

	// Stop jobs for removed tables
	for tableName, entry := range jm.jobMap {
		if !currentTables[tableName] {
			log.Printf("[scheduler] Table removed: stopping job for %s", tableName)
			entry.cancel()
			delete(jm.jobMap, tableName)
		}
	}
}

// -----------------------------------------------------
// startJob: Create a goroutine that auto-refreshes a table
// -----------------------------------------------------
func (jm *JobManager) startJob(parentCtx context.Context, tableName string, interval int) {
	jobCtx, cancel := context.WithCancel(parentCtx)

	jm.jobMap[tableName] = &jobEntry{
		cancel:   cancel,
		interval: interval,
	}

	jm.wg.Add(1)

	go func() {
		defer jm.wg.Done()

		ticker := time.NewTicker(time.Duration(interval) * time.Second)
		defer ticker.Stop()

		log.Printf("[scheduler] Started job for %s (every %d sec)", tableName, interval)

		for {
			select {
			case <-ticker.C:
				jm.runETL(tableName)
			case <-jobCtx.Done():
				log.Printf("[scheduler] Stopped job for %s", tableName)
				return
			}
		}
	}()
}

// -----------------------------------------------------
// runETL: Full ETL cycle for a single table
// -----------------------------------------------------
func (jm *JobManager) runETL(table string) {
	var meta struct {
		DataSourceURL string `db:"data_source_url"`
	}

	err := jm.db.Get(&meta,
		`SELECT data_source_url FROM table_metadata WHERE table_name = $1`,
		table,
	)
	if err != nil {
		log.Printf("[scheduler] Can't load metadata for %s: %v", table, err)
		return
	}

	// 1. Fetch
	rows, err := jm.etl.FetchData(meta.DataSourceURL)
	if err != nil {
		jm.handleETLError(table, "Fetch failed", err)
		return
	}

	// 2. Transform
	rows = jm.etl.TransformPayload(rows)

	// 3. Validate
	validRows, err := jm.etl.ValidatePayload(table, rows)
	if err != nil {
		jm.handleETLError(table, "Validation failed", err)
		return
	}

	// 4. Insert
	count, err := jm.etl.InsertRows(table, validRows)
	if err != nil {
		jm.handleETLError(table, "Insert failed", err)
		return
	}

	// 5. Success
	successMsg := fmt.Sprintf("Inserted %d rows", count)
	jm.etl.WriteRefreshLog(table, "OK", successMsg)
	jm.etl.UpdateMetadataStatus(table, "OK", nil)

	log.Printf("[scheduler] %s refresh OK → %s", table, successMsg)
}

// -----------------------------------------------------
// handleETLError: Helper to log + metadata update
// -----------------------------------------------------
func (jm *JobManager) handleETLError(table, prefix string, err error) {
	msg := fmt.Sprintf("%s: %v", prefix, err)
	log.Printf("[scheduler] %s → %s", table, msg)

	jm.etl.WriteRefreshLog(table, "ERROR", msg)
	jm.etl.UpdateMetadataStatus(table, "ERROR", &msg)
}

// -----------------------------------------------------
// stopAllJobs: Gracefully stop all goroutines
// -----------------------------------------------------
func (jm *JobManager) stopAllJobs() {
	log.Println("[scheduler] Stopping all running jobs...")
	for _, entry := range jm.jobMap {
		entry.cancel()
	}
	jm.wg.Wait()
	log.Println("[scheduler] All jobs stopped.")
}

// -----------------------------------------------------
// Stop: External shutdown
// -----------------------------------------------------
func (jm *JobManager) Stop() {
	if jm.cancel != nil {
		jm.cancel()
	}
}
