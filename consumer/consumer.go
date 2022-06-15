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

	c := NewConsumer(ctx, "Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", "testqueue")

	c.AddRoute("test", test)

	c.Run()
	c.wg.Wait()
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

func (c *Consumer) AddRoute(name string, fn func(context.Context, *azservicebus.ReceivedMessage)) {
	if c.routes[name] != nil {
		panic(fmt.Sprint("Route has already been delcared:", name))
	}
	c.routes[name] = fn
}

func (c *Consumer) Run() {
	for {
		select {
		case <-c.rctx.Done():
			fmt.Println("Exiting loop...")
			return
		default:
			messages, err := c.receiver.ReceiveMessages(c.rctx,
				1,
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
				go func(fn func(context.Context, *azservicebus.ReceivedMessage), m *azservicebus.ReceivedMessage) {
					defer c.wg.Done()
					fn(ctx, m)
					c.receiver.CompleteMessage(ctx, m, nil)
				}(fn, message)
			}
		}
	}
}

func NewConsumer(ctx context.Context, connection string, endpoint string) *Consumer {
	client, err := azservicebus.NewClientFromConnectionString("Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", nil)

	if err != nil {
		panic(err)
	}

	receiver, err := client.NewReceiverForQueue(
		"testqueue",
		nil,
	)

	return &Consumer{
		receiver: receiver,
		wg:       new(sync.WaitGroup),
		rctx:     ctx,
		routes:   make(map[string]func(context.Context, *azservicebus.ReceivedMessage)),
	}
}
