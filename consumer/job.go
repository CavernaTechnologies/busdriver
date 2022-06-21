package main

import (
	"context"
	"sync"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const leeway = 5

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

func (j *Job) Complete(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.receiver.CompleteMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
	return err
}

func (j *Job) Abandon(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.receiver.AbandonMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
	return err
}

func (j *Job) Kill(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.receiver.DeadLetterMessage(ctx, j.Message, nil)
	j.closeDoneChanLocked()
	return err
}

func (j *Job) RenewLock(ctx context.Context) error {
	j.mu.Lock()
	defer j.mu.Unlock()
	err := j.receiver.RenewMessageLock(ctx, j.Message, nil)
	return err
}
