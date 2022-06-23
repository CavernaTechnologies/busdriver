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

func NewConsumer(receiver *azservicebus.Receiver) *Consumer {
	return &Consumer{
		receiver:    receiver,
		handlers:    make(map[string]func(context.Context, *Job)),
		maxJobs:     100,
		currentJobs: 0,
		mu:          sync.Mutex{},
		running:     false,
	}
}

type Consumer struct {
	receiver *azservicebus.Receiver
	handlers map[string]func(context.Context, *Job)

	maxJobs     uint32
	currentJobs uint32

	mu      sync.Mutex
	running bool
}

func (c *Consumer) AddHandler(name string, fn func(context.Context, *Job)) error {
	c.mu.Lock()
	defer c.mu.Unlock()
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

func (c *Consumer) Run(ctx context.Context) error {
	if c.receiver == nil {
		return &NilReceiverErr{
			info: "Receiver must be none nil",
		}
	}

	c.mu.Lock()
	if c.running == true {
		c.mu.Unlock()
		return &RunningErr{
			info: "Consumer already running",
		}
	}
	c.running = true
	c.mu.Unlock()
	defer c.stopRunning()

	for {
		select {
		case <-ctx.Done():
			return &FinishedErr{
				info: "Context canceled",
			}
		default:
		}

		d := c.getJobsDelta()

		if d == 0 {
			time.Sleep(100 * time.Millisecond)
			continue
		}

		messages, err := c.receiver.ReceiveMessages(ctx, int(d), nil)

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return &FinishedErr{
				info: "Context canceled",
			}
		}

		if err != nil {
			return &ReceiveErr{
				info: "Failed to receive messages",
			}
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

			c.incCurrentJobs()
			go func(message *azservicebus.ReceivedMessage) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				defer c.decCurrentJobs()

				j := &Job{
					receiver: c.receiver,
					Message:  message,
				}

				fn(ctx, j)

				defer func() {
					if err := recover(); err != nil {
						fmt.Println("Recovered from panic. Killing message...\nErr:", err)
						j.Kill(ctx)
					}
				}()
			}(message)
		}
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

func (c *Consumer) stopRunning() {
	c.mu.Lock()
	c.running = false
	c.mu.Unlock()
}
