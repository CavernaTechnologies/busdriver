package main

import (
	"context"
	"errors"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type Consumer struct {
	receiver *azservicebus.Receiver
	handlers map[string]func(context.Context, *Job)
}

func (c *Consumer) AddHandler(name string, fn func(context.Context, *Job)) error {
	if c.handlers == nil {
		return errors.New("handlers has not been declared")
	}

	if _, exists := c.handlers[name]; exists {
		return errors.New("Handler name already exists")
	}

	c.handlers[name] = fn
	return nil
}

func (c *Consumer) Run(ctx context.Context) error {
	if c.receiver == nil {
		return errors.New("receiver must be defined")
	}

	for {
		select {
		case <-ctx.Done():
			return errors.New("Consumer finished")
		default:
		}

		messages, err := c.receiver.ReceiveMessages(ctx, 1, nil)

		if err != nil {
			return errors.New("Failed to receive messages")
		}

		for _, message := range messages {
			ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()

			if message.Subject == nil {
				r := "No message subject"
				c.receiver.DeadLetterMessage(ctx, message, &azservicebus.DeadLetterOptions{
					Reason: &r,
				})
			}

			fn, ok := c.handlers[*message.Subject]

			if !ok {
				r := "No handler found"
				c.receiver.DeadLetterMessage(ctx, message, &azservicebus.DeadLetterOptions{
					Reason: &r,
				})
			}

			go func(message *azservicebus.ReceivedMessage) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()

				j := &Job{
					receiver: c.receiver,
					Message:  message,
				}

				fn(ctx, j)
			}(message)
		}
	}
}

func NewConsumer(receiver *azservicebus.Receiver) *Consumer {
	return &Consumer{
		receiver: receiver,
		handlers: make(map[string]func(context.Context, *Job)),
	}
}
