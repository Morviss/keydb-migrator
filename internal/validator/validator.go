package validator

import (
	"context"
	"fmt"
	"keydb-migrator/internal/client"
	"keydb-migrator/internal/logger"
)

type Validator struct {
	sourceClient      *client.KeyDBClient
	destinationClient *client.KeyDBClient
	logger            logger.Logger
}

//Cereate an new validator instance

func New(source, dist *client.KeyDBClient, log logger.Logger) *Validator {
	return &Validator{
		sourceClient:      source,
		destinationClient: dist,
		logger:            log,
	}
}

//validate key check all the keys present in the both the databases to match the data

func (v *Validator) Validatekey(ctx context.Context, key string) error {
	//checking the key existance
	exists, err := v.destinationClient.Exists(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to check the key existance in the destination keydb %w", err)
	}

	if exists == 0 {

	}
	return nil
}
