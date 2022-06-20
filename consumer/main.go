package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

func main() {
	client, err := azservicebus.NewClientFromConnectionString("Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", nil)

	if err != nil {
		panic(err)
	}

	receiver, err := client.NewReceiverForQueue("testqueue", nil)

	if err != nil {
		panic(err)
	}

	ctx := context.Background()

	messages, err := receiver.ReceiveMessages(ctx, 1, nil)

	if err != nil {
		panic(err)
	}

	var job *Job

	for _, message := range messages {
		job = &Job{
			receiver: receiver,
			Message:  message,
			mu:       sync.Mutex{},
		}
	}

	fmt.Println(*job.Message.Subject)

	job.keepAlive(ctx)

	time.Sleep(45 * time.Second)

	job.Complete(ctx)

	time.Sleep(20 * time.Second)
}
