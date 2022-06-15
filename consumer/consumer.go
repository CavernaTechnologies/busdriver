package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	client, err := azservicebus.NewClientFromConnectionString("Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", nil)

	if err != nil {
		panic(err)
	}

	receiver, err := client.NewReceiverForQueue(
		"testqueue",
		nil,
	)

	messages := make(chan *azservicebus.ReceivedMessage, 10)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go consumer(stop, wg, receiver, messages)
	go router(stop, wg, receiver, messages)

	exit := make(chan os.Signal, 1)
	signal.Notify(exit, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-exit
		close(stop)
	}()
	wg.Wait()
}

func router(stop <-chan struct{}, wg sync.WaitGroup, receiver *azservicebus.Receiver, messages <-chan *azservicebus.ReceivedMessage) {
	defer wg.Done()
	ctx := context.Background()

	for message := range messages {
		select {
		case <-stop:
			err := receiver.AbandonMessage(ctx, message, nil)

			if err != nil {
				fmt.Println(err)
			}
		default:
			b := string(message.Body)
			fmt.Println(b)

			err := receiver.CompleteMessage(ctx, message, nil)

			if err != nil {
				fmt.Println(err)
			}
		}
	}
}

func consumer(stop <-chan struct{}, wg sync.WaitGroup, receiver *azservicebus.Receiver, messages chan<- *azservicebus.ReceivedMessage) {
	defer wg.Done()
	ctx := context.Background()

	for {
		select {
		case <-stop:
			close(messages)
			return
		default:
			m, err := receiver.ReceiveMessages(ctx,
				10,
				nil,
			)

			if err != nil {
				panic(err)
			}

			for _, message := range m {
				messages <- message
			}
		}
	}
}
