package main

import (
	"context"
	"fmt"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
	"github.com/CavernaTechnologies/busdriver"
)

func main() {
	fmt.Println("Logging in...")

	credential, err := azidentity.NewDefaultAzureCredential(nil)

	if err != nil {
		panic(err)
	}

	fmt.Println("Connecting to client...")

	client, err := azservicebus.NewClient("cavernatesting.servicebus.windows.net", credential, nil)

	if err != nil {
		panic(err)
	}

	fmt.Println("Connecting to queue...")

	receiver, err := client.NewReceiverForQueue(
		"testqueue",
		&azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		},
	)

	if err != nil {
		panic(err)
	}

	fmt.Println("Creating consumer...")

	c := busdriver.NewConsumer(receiver)

	c.AddHandler("test", test)

	fmt.Println("Running...")

	ctx := context.Background()
	err = c.Run(ctx)

	if err != nil {
		panic(err)
	}
}

func test(ctx context.Context, j *busdriver.Job) {
	fmt.Println(*j.Message.Subject)
	fmt.Println(string(j.Message.Body))

	j.Complete(ctx)
}
