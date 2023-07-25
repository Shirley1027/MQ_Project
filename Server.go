package main

import (
	"context"
	"fmt"
	"github.com/apache/rocketmq-client-go/v2"
	"github.com/apache/rocketmq-client-go/v2/primitive"
	"github.com/apache/rocketmq-client-go/v2/producer"
	"os"
	"sync"
)

// 简单的生产者
func main() {
	p, _ := rocketmq.NewProducer(
		//  用于设置NameServer地址解析器，NewPassthroughResolver指定NameServer地址
		producer.WithNsResolver(primitive.NewPassthroughResolver([]string{"192.168.1.9:9876"})),
		//  设置重发次数
		producer.WithRetry(3),
		producer.WithGroupName("testGroup"), // 分组名称
	)
	err := p.Start()
	if err != nil {
		fmt.Printf("start producer error: %s", err.Error())
		os.Exit(1)
	}
	defer p.Shutdown()

	topic := "Consumer_Cluster"

	syncSend(topic, p)

}

func syncSend(topic string, p rocketmq.Producer) {

	// 创建消息
	msgs := []*primitive.Message{
		{
			Topic: topic,                              // 消息主题
			Body:  []byte("Hello RocketMQ Go Client"), // 消息内容
		},
		{
			Topic: topic,                                    // 消息主题
			Body:  []byte("Hello RocketMQ Go Client again"), // 消息内容
		},
	}
	//msg.WithTag("my-tag")
	//msg.WithKeys([]string{"my-key"})
	//// 通过分区消息来保证消息的有序性，相同orderID消息需要保证顺序，不同ID不需要
	//orderId := strconv.Itoa(i % 10)

	// 设置消息的分区键，用于指定消息应该发送到哪个分区
	//msg.WithShardingKey(orderId)

	// SendSync()是同步发送消息的方法，会阻塞等待消息发送完成并返回发送结果
	// context.Background为空的上下文，用于发送消息的超时控制和取消操作
	for i := 0; i < 100; i++ {
		for _, msg := range msgs {
			res, err := p.SendSync(context.Background(), msg)
			if err != nil {
				fmt.Printf("send message error : %s\n", err)
				continue
			} else {
				fmt.Printf("send message success: result=%s\n", res.String())
			}
		}
	}

}

func asyncSend(topic string, p rocketmq.Producer) {
	var wg sync.WaitGroup
	wg.Add(1)

	callback := func(ctx context.Context, result *primitive.SendResult, e error) {
		if e != nil {
			fmt.Printf("receive message error: %s\n", e)
		} else {
			fmt.Printf("send message success: result=%s\n", result.String())
		}
		wg.Done()
	}

	message := primitive.NewMessage(topic, []byte("Hello RocketMQ Go Client!"))
	err := p.SendAsync(context.Background(), callback, message)
	if err != nil {
		fmt.Printf("send message error : %s/n", err)
		wg.Done()
	}

	wg.Wait()
}
