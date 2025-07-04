package stats

import (
	"context"
	"keydb-migrator/internal/logger"
	"sync"
	"time"
)

// stats holds migration statics
type Stats struct {
	totalkeys    int64
	migratedKeys int64
	failedkeys   int64
	startTime    time.Time
	mu           sync.RWMutex
}

// create new stats instance
func Newstats() *Stats {
	return &Stats{
		startTime: time.Now(),
	}
}

// settotal keys sets the total no of keys
func (s *Stats) SetTotalKeys(total int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.totalkeys = total
}

// increament migrated keys increases migrated keys counter
func (s *Stats) IncreamentmigratedKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.migratedKeys++
}

// increament failed keys
func (s *Stats) IncreamentFailedKeys() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.failedkeys++
}

// get current results
func (s *Stats) GetStats() (total, migrated, failed int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.totalkeys, s.migratedKeys, s.failedkeys
}

// startprogresssionreprot starts predoic progress for the report
func (s *Stats) StartProgressReporter(ctx context.Context, logger logger.Logger) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			total, migrated, failed := s.GetStats()
			elapsed := time.Since(s.startTime)
			rate := float64(migrated) / elapsed.Seconds()

			logger.Info(
				"migration Progress",
				"total", total,
				"migrated", migrated,
				"failed", failed,
				"rate_per_sec", rate,
				"elapsed", elapsed,
			)
		case <-ctx.Done():
			return
		}
	}
}

// final log for the stats for migration
func (s *Stats) LogFinalStats(logger logger.Logger) {
	total, migrated, failed := s.GetStats()
	elasedTime := time.Since(s.startTime)
	logger.Info("Migration completed",
		"total_keys", total,
		"migrated_keys", migrated,
		"failed_keys", failed,
		"duration", elasedTime,
		"success_rate", float64(migrated)/float64(total)*100,
	)
}
