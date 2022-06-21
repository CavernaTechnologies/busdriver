package main

import (
	"context"
	"fmt"
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

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Second)
	defer cancel()

	c := NewConsumer(receiver)

	c.AddHandler("test", test)

	c.Run(ctx)
}

func test(ctx context.Context, j *Job) {
	err := j.KeepAlive(ctx)

	if err != nil {
		return
	}

	time.Sleep(60 * time.Second)

	fmt.Println(string(j.Message.Body))
	j.Complete(ctx)
}
