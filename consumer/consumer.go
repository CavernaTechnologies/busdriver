package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	exit := make(chan os.Signal)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		cancel()
	}()

	client, err := azservicebus.NewClientFromConnectionString("Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", nil)

	if err != nil {
		panic(err)
	}

	c := NewConsumer(ctx, client, "testqueue")

	c.AddRoute("test", test)

	c.Run()
}

func test(ctx context.Context, message *azservicebus.ReceivedMessage) {
	fmt.Println("Subject:", *message.Subject)

	body := string(message.Body)
	fmt.Println(body)
	fmt.Println("Processing...")
	time.Sleep(3 * time.Second)
	fmt.Println(*message.Subject, "done")
}

type Consumer struct {
	receiver *azservicebus.Receiver
	wg       *sync.WaitGroup
	rctx     context.Context
	routes   map[string]func(context.Context, *azservicebus.ReceivedMessage)
}

func (c *Consumer) Wait() {
	c.wg.Wait()
}

func (c *Consumer) AddRoute(name string, fn func(context.Context, *azservicebus.ReceivedMessage)) {
	if c.routes[name] != nil {
		panic(fmt.Sprint("Route has already been delcared:", name))
	}
	c.routes[name] = fn
}

func (c *Consumer) runStandard() {
	for {
		select {
		case <-c.rctx.Done():
			fmt.Println("Exiting loop...")
			return
		default:
			messages, err := c.receiver.ReceiveMessages(c.rctx,
				10,
				nil,
			)

			if err != nil {
				fmt.Println(err)
			}

			for _, message := range messages {
				sub := message.Subject
				ctx := context.Background()

				if sub == nil {
					c.receiver.DeadLetterMessage(ctx, message, nil)
					continue
				}

				fn, exists := c.routes[*sub]
				if !exists {
					c.receiver.DeadLetterMessage(ctx, message, nil)
					continue
				}

				c.wg.Add(1)
				go func(m *azservicebus.ReceivedMessage) {
					defer c.wg.Done()
					fn(ctx, m)
					c.receiver.CompleteMessage(ctx, m, nil)
				}(message)
			}
		}
	}
}

func (c *Consumer) Run() {
	c.runStandard()
	c.Wait()
}

func NewConsumer(ctx context.Context, client *azservicebus.Client, queueName string) *Consumer {
	receiver, err := client.NewReceiverForQueue(
		queueName,
		nil,
	)

	if err != nil {
		panic(err)
	}

	return &Consumer{
		receiver: receiver,
		wg:       new(sync.WaitGroup),
		rctx:     ctx,
		routes:   make(map[string]func(context.Context, *azservicebus.ReceivedMessage)),
	}
}
