package main

import (
	"bufio"
	"context"
	"fmt"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

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

	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("Enter Text: ")
		// reads user input until \n by default
		scanner.Scan()
		// Holds the string that was scanned
		bytes := scanner.Bytes()
		if len(bytes) != 0 {
			message := &azservicebus.Message{
				Body: bytes,
			}

			err = sender.SendMessage(context.TODO(), message, nil)

			if err != nil {
				panic(err)
			}
		} else {
			// exit if user entered an empty string
			break
		}

	}

	// handle error
	if scanner.Err() != nil {
		fmt.Println("Error: ", scanner.Err())
	}
}
