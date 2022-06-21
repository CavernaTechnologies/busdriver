package main

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

type Consumer struct {
	MaxJobs uint

	receiver *azservicebus.Receiver
	handlers map[string]func(context.Context, *Job)

	mu      sync.Mutex
	numJobs uint
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

		n := c.getNumJobs()
		max := c.MaxJobs

		if n >= max {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		messages, err := c.receiver.ReceiveMessages(ctx, int(max-n), nil)

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

			c.incrementJobs()
			go func(message *azservicebus.ReceivedMessage) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				defer c.decrementJobs()

				j := &Job{
					receiver: c.receiver,
					Message:  message,
				}

				fn(ctx, j)
			}(message)
		}
	}
}

func (c *Consumer) getNumJobs() uint {
	c.mu.Lock()
	j := c.numJobs
	c.mu.Unlock()
	return j
}

func (c *Consumer) incrementJobs() {
	c.mu.Lock()
	c.numJobs += 1
	c.mu.Unlock()
}

func (c *Consumer) decrementJobs() {
	c.mu.Lock()
	c.numJobs -= 1
	c.mu.Unlock()
}

func NewConsumer(receiver *azservicebus.Receiver) *Consumer {
	return &Consumer{
		MaxJobs:  100,
		receiver: receiver,
		handlers: make(map[string]func(context.Context, *Job)),
		mu:       sync.Mutex{},
		numJobs:  0,
	}
}
