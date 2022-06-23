package main

import (
	"context"
	"fmt"
	"time"

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

	if err != nil {
		panic(err)
	}

	fmt.Println("Creating consumer...")

	c, err := busdriver.NewConsumerForQueue(
		client,
		"testqueue",
		&azservicebus.ReceiverOptions{
			ReceiveMode: azservicebus.ReceiveModePeekLock,
		},
	)

	c.AddHandler("test", test)
	c.AddHandler("longTest", longTest)
	c.AddHandler("panicTest", panicTest)

	fmt.Println("Running...")

	err = c.Run()

	if err != nil {
		panic(err)
	}
}

func test(ctx context.Context, j *busdriver.Job) {
	fmt.Println(*j.Message.Subject)
	fmt.Println(string(j.Message.Body))

	j.Complete(ctx)
}

func longTest(ctx context.Context, j *busdriver.Job) {
	fmt.Println("Starting long test...")

	time.Sleep(10 * time.Second)

	fmt.Println(*j.Message.Subject)
	fmt.Println(string(j.Message.Body))

	j.Complete(ctx)
}

func panicTest(ctx context.Context, j *busdriver.Job) {
	panic("PANICING VIOLENTLY")
}
