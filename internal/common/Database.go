package common

import "github.com/go-redis/redis/v8"

var (
	RedisCli *redis.Client
)

func initRedisClient() {
	RedisCli = redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	})
}

func GetRedisCli() *redis.Client {
	if RedisCli == nil {
		initRedisClient()
		return RedisCli
	}
	return RedisCli
}
