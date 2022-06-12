package main

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const qrl = "cavernatesting.servicebus.windows.net"

func main() {
	client, err := azservicebus.NewClientFromConnectionString("Endpoint=sb://cavernatesting.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=oXi4UpGB9/cQ4j47Fg4iaG5YiHyHKZHXrYf2M3/ySvo=", nil)

	if err != nil {
		panic(err)
	}

	sender, err := client.NewSender("testqueue", nil)

	if err != nil {
		panic(err)
	}

	defer sender.Close(context.TODO())

	message := &azservicebus.Message{
		Body: []byte("HI"),
	}

	err = sender.SendMessage(context.TODO(), message, nil)

	if err != nil {
		panic(err)
	}
}
