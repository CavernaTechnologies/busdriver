package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const interval = 20

type Job struct {
	receiver *azservicebus.Receiver
	Message  *azservicebus.ReceivedMessage

	mu       sync.Mutex
	doneChan chan struct{}
}

func (j *Job) getDoneChan() <-chan struct{} {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.getDoneChanLocked()
}

func (j *Job) getDoneChanLocked() chan struct{} {
	if j.doneChan == nil {
		j.doneChan = make(chan struct{})
	}
	return j.doneChan
}

func (j *Job) closeDoneChanLocked() {
	ch := j.getDoneChanLocked()
	select {
	case <-ch:
		// Already closed. Don't close again.
	default:
		close(ch)
	}
}

func (j *Job) Complete(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.receiver.CompleteMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
}

func (j *Job) Abandon(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.receiver.AbandonMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
}

func (j *Job) Kill(ctx context.Context) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.receiver.DeadLetterMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
}

func (j *Job) keepAlive(ctx context.Context) {
	go func() {
		for {
			time.Sleep(interval * time.Second)
			j.mu.Lock()
			select {
			case <-j.getDoneChanLocked():
				fmt.Println("Done")
				j.mu.Unlock()
				return
			case <-ctx.Done():
				fmt.Println("Canceled")
				j.mu.Unlock()
				return
			default:
			}
			fmt.Println("Renewing....")
			j.receiver.RenewMessageLock(ctx, j.Message, nil)
			j.mu.Unlock()
		}
	}()
}
