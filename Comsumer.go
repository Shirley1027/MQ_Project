package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/consumer"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"mq_proj/internal/common"
	"os"
	"sync"
	"time"
)

func main() {
	sig := make(chan os.Signal)
	c, _ := rocketmq.NewPushConsumer(
		consumer.WithGroupName("testGroup"),
		consumer.WithNsResolver(primitive.NewPassthroughResolver([]string{"192.168.1.9:9876"})),
		consumer.WithConsumerModel(consumer.Clustering),
	)
	err := c.Subscribe("Consumer_Cluster", consumer.MessageSelector{}, func(ctx context.Context,
		msgs ...*primitive.MessageExt) (consumer.ConsumeResult, error) {
		for _, msg := range msgs {
			if checkDuplicateRedis(msg) {
				fmt.Println("Duplicate message, skip.")
				continue
			}
			processMessageRedis(msg)
			saveMessageIDRedis(msg)
		}
		return consumer.ConsumeSuccess, nil
	})
	if err != nil {
		fmt.Printf(err.Error())
	}

	err = c.Start()
	if err != nil {
		fmt.Println(err.Error())
		os.Exit(-1)
	}

	<-sig
	err = c.Shutdown()

	if err != nil {
		fmt.Printf("shutdown Consumer error: %s", err.Error())
	}
}

var (
	messageIDs = make(map[string]struct{})
	mutex      = sync.Mutex{}
	redisCli   = common.GetRedisCli()
)

func checkDuplicate(msg *primitive.MessageExt) bool {
	key := msg.MsgId
	mutex.Lock()
	defer mutex.Unlock()
	if _, ok := messageIDs[key]; ok {
		return true
	}
	return false
}

func saveMessageID(msgID string) {
	key := msgID
	mutex.Lock()
	defer mutex.Unlock()
	messageIDs[key] = struct{}{}
}

func processMessage(msg *primitive.MessageExt) {
	fmt.Printf("Received message: %s\n", string(msg.Body))
}

func checkDuplicateRedis(msg *primitive.MessageExt) bool {
	//获取Redis cli
	redidCli := common.GetRedisCli()

	exists, err := redidCli.Exists(context.Background(), msg.MsgId).Result()
	if err != nil {
		fmt.Println("Error checking for message existence in Redis:", err)
		return false
	}

	return exists == 1
}

func saveMessageIDRedis(msg *primitive.MessageExt) {
	// 存储消息到redis中并设置过期时间
	key := msg.MsgId
	err := redisCli.Set(context.Background(), key, string(msg.Body), time.Minute).Err() // 设置过期时间为1天
	if err != nil {
		fmt.Println("Error storing message in Redis:", err)
	}
}

func processMessageRedis(msg *primitive.MessageExt) {
	// 在这里处理收到的消息
	fmt.Printf("Received message: %s\n", string(msg.Body))
}
