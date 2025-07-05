package migrator

import (
	"context"
	"fmt"
	"keydb-migrator/internal/client"
	"keydb-migrator/internal/config"
	"keydb-migrator/internal/logger"
	"keydb-migrator/internal/stats"
)

//migratoe struct for the migration struct

type Migrator struct {
	config            *config.Config
	sourceClient      *client.KeyDBClient
	destinationClient *client.KeyDBClient
	logger            logger.Logger
	stats             *stats.Stats
}

// creates the new migrator insatnce

func New(cfg *config.Config, log logger.Logger) (*Migrator, error) {
	//creates source client
	sourceClient, err := client.New(cfg.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to create source client: %w", err)
	}

	//create destination cliient
	destinationClient, err := client.New(cfg.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination client: %w", err)
	}

	return &Migrator{
		config:            cfg,
		sourceClient:      sourceClient,
		destinationClient: destinationClient,
		logger:            log,
		stats:             stats.Newstats(),
	}, nil
}

// migrate promformance the mogration
func (m *Migrator) Migrate(ctx context.Context) error {
	m.logger.Info("starting the migrarion process.")

	return nil
}

// test all the connection with the source and destination
func (m *Migrator) testconnections(ctx context.Context) error {
	m.logger.Info("testing the connections with keydbs")
	if err := m.sourceClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping the source keydb %w", err)
	}

	if err := m.destinationClient.Ping(ctx); err != nil {
		return fmt.Errorf("failed to ping the destination keydb %w", err)
	}

	m.logger.Info("Both keydbs are live.")

	return nil

}

//reteriving keys from the keydb

func (m *Migrator) GetKeys(ctx context.Context) ([]string, error) {
	var keys []string
	iter := m.sourceClient.Scan(ctx, 0, "*", int64(m.config.Migration.BatchSize)).Iterator()
	for iter.Next(ctx) {
		keys = append(keys, iter.Val())
	}

	return keys, nil
}
