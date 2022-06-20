package main

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const interval = 20

type Job struct {
	receiver *azservicebus.Receiver
	doneChan chan struct{}
	Message  *azservicebus.ReceivedMessage
}

func (j *Job) Complete(ctx context.Context) {
	j.receiver.CompleteMessage(ctx, j.Message, nil)
}

func (j *Job) Abandon(ctx context.Context) {
	j.receiver.AbandonMessage(ctx, j.Message, nil)
}

func (j *Job) Kill(ctx context.Context) {
	j.receiver.DeadLetterMessage(ctx, j.Message, nil)
}

func (j *Job) keepAlive(ctx context.Context) {

}
