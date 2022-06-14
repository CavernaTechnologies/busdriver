package main

import (
	"context"
	"errors"
	"fmt"

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

	for {
		ctx := context.Background()

		messages, err := receiver.ReceiveMessages(ctx,
			10,
			nil,
		)

		if err != nil {
			panic(err)
		}

		if len(messages) == 0 {
			break
		}

		for _, message := range messages {
			// The message body is a []byte. For this example we're just assuming that the body
			// was a string, converted to bytes but any []byte payload is valid.
			var body []byte = message.Body
			fmt.Printf("Message received with body: %s\n", string(body))

			// For more information about settling messages:
			// https://docs.microsoft.com/azure/service-bus-messaging/message-transfers-locks-settlement#settling-receive-operations
			err = receiver.CompleteMessage(ctx, message, nil)

			if err != nil {
				var sbErr *azservicebus.Error

				if errors.As(err, &sbErr) && sbErr.Code == azservicebus.CodeLockLost {
					// The message lock has expired. This isn't fatal for the client, but it does mean
					// that this message can be received by another Receiver (or potentially this one!).
					fmt.Printf("Message lock expired\n")

					// You can extend the message lock by calling receiver.RenewMessageLock(msg) before the
					// message lock has expired.
					continue
				}

				panic(err)
			}

			fmt.Printf("Received and completed the message\n")
		}
	}
}
