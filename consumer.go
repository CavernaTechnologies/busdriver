package busdriver

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

// Create a consumer for a queue
func NewConsumerForQueue(client *azservicebus.Client, queueName string, options *azservicebus.ReceiverOptions) (*Consumer, error) {
	receiver, err := client.NewReceiverForQueue(queueName, options)

	if err != nil {
		return nil, err
	}

	return &Consumer{
		receiver:    receiver,
		handlers:    make(map[string]func(context.Context, *Job)),
		maxJobs:     100,
		currentJobs: 0,
		runMu:       sync.Mutex{},
		running:     false,
	}, nil
}

// Create a consumer for a subscription
func NewConsumerForSubscription(client *azservicebus.Client, topicName string, subName string, options *azservicebus.ReceiverOptions) (*Consumer, error) {
	receiver, err := client.NewReceiverForSubscription(topicName, subName, options)

	if err != nil {
		return nil, err
	}

	return &Consumer{
		receiver:    receiver,
		handlers:    make(map[string]func(context.Context, *Job)),
		maxJobs:     100,
		currentJobs: 0,
		runMu:       sync.Mutex{},
		running:     false,
	}, nil
}

type Consumer struct {
	// can be a queue or subscription receiver
	receiver *azservicebus.Receiver

	// the functions to be executed upon receiving a message
	handlers map[string]func(context.Context, *Job)

	maxJobs     uint32
	currentJobs uint32

	// controls access to the runtime variables
	runMu     sync.Mutex
	running   bool
	cancelCtx func()
}

// Add a message handler to the consumer
func (c *Consumer) AddHandler(name string, fn func(context.Context, *Job)) error {
	c.runMu.Lock()
	defer c.runMu.Unlock()
	if c.running {
		return &RunningErr{
			info: "Cannot add handler while consumer is running",
		}
	}

	if c.handlers == nil {
		return errors.New("handlers has not been declared")
	}

	if _, exists := c.handlers[name]; exists {
		return errors.New("Handler name already exists")
	}

	c.handlers[name] = fn
	return nil
}

// Run consumer
func (c *Consumer) Run() error {
	if c.receiver == nil {
		return &NilReceiverErr{
			info: "Receiver must be none nil",
		}
	}

	c.runMu.Lock()
	if c.running == true {
		c.runMu.Unlock()
		return &RunningErr{
			info: "Consumer already running",
		}
	}
	c.running = true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c.cancelCtx = cancel
	c.runMu.Unlock()

	defer c.Stop()

	for {
		// Check if context is still alive. If not, exit
		select {
		case <-ctx.Done():
			return &FinishedErr{
				info: "Context canceled",
			}
		default:
		}

		// Get number of jobs before concurrency limit
		d := c.getJobsDelta()

		// If we are at the concurrency limit, sleep and then continue
		if d == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		messages, err := c.receiver.ReceiveMessages(ctx, int(d), nil)

		// If the context is canceled, exit
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &FinishedErr{
				info: "Context canceled",
			}
		}

		// If we have an error receiving messages, exit
		if err != nil {
			return &ReceiveErr{
				info: "Failed to receive messages",
			}
		}

		for _, message := range messages {
			// If there is no message subject, kill message
			if message.Subject == nil {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				r := "No message subject"
				c.receiver.DeadLetterMessage(ctx, message, &azservicebus.DeadLetterOptions{
					Reason: &r,
				})
				cancel()
			}

			fn, ok := c.handlers[*message.Subject]

			// If there is no handler matching the message subject, kill message
			if !ok {
				ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
				r := "No handler found"
				c.receiver.DeadLetterMessage(ctx, message, &azservicebus.DeadLetterOptions{
					Reason: &r,
				})
				cancel()
			}

			c.incCurrentJobs()
			go func(message *azservicebus.ReceivedMessage) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				defer c.decCurrentJobs()

				// If the function panics, recover, kill message
				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Recovered from panic. Killing message...\nErr:", err)
						c.receiver.DeadLetterMessage(ctx, message, nil)
					}
				}()

				j := &Job{
					receiver: c.receiver,
					Message:  message,
				}

				fn(ctx, j)
			}(message)
		}
	}
}

// Stops the consumer. It may be restarted after stopping
func (c *Consumer) Stop() error {
	c.runMu.Lock()
	defer c.runMu.Unlock()

	if !c.running {
		return errors.New("Consumer is not running")
	}

	c.cancelCtx()
	c.running = false
	return nil
}

// Terminates the consumer. This immediately and permanently closes all connections with Azure Service Bus
func (c *Consumer) Terminate(ctx context.Context) error {
	return c.receiver.Close(ctx)
}

func (c *Consumer) Wait(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		if !c.running && c.getCurrentJobs() == 0 {
			return
		}

		time.Sleep(500 * time.Millisecond)
	}
}

func (c *Consumer) getCurrentJobs() uint32 {
	return atomic.LoadUint32(&c.currentJobs)
}

func (c *Consumer) incCurrentJobs() {
	atomic.AddUint32(&c.currentJobs, 1)
}

func (c *Consumer) decCurrentJobs() {
	atomic.AddUint32(&c.currentJobs, ^uint32(0))
}

func (c *Consumer) SetMaxJobs(n uint32) {
	atomic.StoreUint32(&c.maxJobs, n)
}

func (c *Consumer) GetMaxJobs() uint32 {
	return atomic.LoadUint32(&c.maxJobs)
}

func (c *Consumer) getJobsDelta() uint32 {
	cur := c.getCurrentJobs()
	max := c.GetMaxJobs()

	if cur >= max {
		return 0
	}

	return max - cur
}
