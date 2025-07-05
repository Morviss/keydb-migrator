package migrator

import (
	"context"
	"time"
)

//migration of diffrent types of the keys and ttl handling

func (m *Migrator) migrateString(ctx context.Context, key string, ttl time.Duration) error {
	val, err := m.sourceClient.Get(ctx, key).Result()
	if err != nil {
		return err
	}
	return m.destinationClient.Set(ctx, key, val, ttl).Err()
}

func (m *Migrator) migrateHash(ctx context.Context, key string, ttl time.Duration) error {
	hash, err := m.sourceClient.HGetAll(ctx, key).Result()
	if err != nil {
		return err
	}

	if len(hash) > 0 {
		args := make([]interface{}, 0, len(hash)*2)

		for feild, val := range hash {
			args = append(args, feild, val)
		}
		if err := m.destinationClient.HMSet(ctx, key, args...).Err(); err != nil {
			return nil
		}
	}
	if ttl > 0 {
		return m.destinationClient.Expire(ctx, key, ttl).Err()
	}

	return nil
}


