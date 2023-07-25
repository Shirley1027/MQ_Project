package common

import (
	"context"
	"github.com/go-redis/redis/v8"
	"time"
)

const (
	MessageTTLKey = "MessageTTLKey"
)

// 给消息设置过期时间
func SetTTLToMessage(redisDB *redis.Client) error {
	err := redisDB.Set(context.Background(), MessageTTLKey, "MessageTTL", time.Minute).Err()
	if err != nil {
		return err
	}
	return nil
}
