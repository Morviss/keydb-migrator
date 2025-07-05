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
		return fmt.Errorf("key %s does not exist in the destination keydb", key)
	}

	//compare types
	sourceType, err := v.sourceClient.Type(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key type from the source keydb from the key %s", key)
	}

	distType, err := v.destinationClient.Type(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key type from the dist keydb from the key %s", key)
	}
	if sourceType != distType {
		return fmt.Errorf("key type mismatch: %s (source) != %s (destination)", sourceType, distType)
	}

	//validate content based on the Type
	switch sourceType {
	case "string":
		return v.ValidateString(ctx, key)
	case "hash":
		return v.ValidateHget(ctx, key)
	case "list":
		return v.validateList(ctx, key)
	case "set":
		return v.validateSet(ctx, key)
	case "zset":
		return v.validateZSet(ctx, key)
	default:
		return fmt.Errorf("validation not supported for type: %s", sourceType)
	}
}

// validation types of the keys in this utility
func (v *Validator) ValidateString(ctx context.Context, key string) error {
	srcVal, err := v.sourceClient.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key from the source value from the source keydb ,%w", err)
	}

	distVal, err := v.destinationClient.Get(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key from the dist from the destination keydb,%w", err)
	}

	if srcVal != distVal {
		return fmt.Errorf("value mismatch for the key %s", key)
	}

	return nil
}

func (v *Validator) ValidateHget(ctx context.Context, key string) error {
	srcVal, err := v.sourceClient.HGetAll(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key from the source value from the source keydb ,%w", err)
	}

	distVal, err := v.destinationClient.HGetAll(ctx, key).Result()
	if err != nil {
		return fmt.Errorf("failed to get the key from the dist from the destination keydb,%w", err)
	}

	if len(srcVal) != len(distVal) {
		return fmt.Errorf("mismatch of key %s", key)
	}

	for k, v1 := range srcVal {
		if v2, ok := distVal[k]; !ok || v1 != v2 {
			return fmt.Errorf("hash field mismatch for key %s, field %s", key, k)
		}
	}

	return nil
}

func (v *Validator) validateList(ctx context.Context, key string) error {
	srcList, err := v.sourceClient.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return err
	}
	destList, err := v.destinationClient.LRange(ctx, key, 0, -1).Result()
	if err != nil {
		return err
	}
	if len(srcList) != len(destList) {
		return fmt.Errorf("list length mismatch for key %s", key)
	}
	for i := range srcList {
		if srcList[i] != destList[i] {
			return fmt.Errorf("list mismatch for key %s at index %d", key, i)
		}
	}
	return nil
}

func (v *Validator) validateSet(ctx context.Context, key string) error {
	srcSet, err := v.sourceClient.SMembers(ctx, key).Result()
	if err != nil {
		return err
	}
	destSet, err := v.destinationClient.SMembers(ctx, key).Result()
	if err != nil {
		return err
	}
	if len(srcSet) != len(destSet) {
		return fmt.Errorf("set length mismatch for key %s", key)
	}
	srcMap := make(map[string]bool)
	for _, val := range srcSet {
		srcMap[val] = true
	}
	for _, val := range destSet {
		if !srcMap[val] {
			return fmt.Errorf("missing set member %s for key %s", val, key)
		}
	}
	return nil
}

func (v *Validator) validateZSet(ctx context.Context, key string) error {
	srcZSet, err := v.sourceClient.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return err
	}
	destZSet, err := v.destinationClient.ZRangeWithScores(ctx, key, 0, -1).Result()
	if err != nil {
		return err
	}
	if len(srcZSet) != len(destZSet) {
		return fmt.Errorf("zset length mismatch for key %s", key)
	}
	for i := range srcZSet {
		if srcZSet[i].Member != destZSet[i].Member || srcZSet[i].Score != destZSet[i].Score {
			return fmt.Errorf("zset member mismatch for key %s at index %d", key, i)
		}
	}
	return nil
}
