package busdriver

import (
	"context"
	"fmt"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/messaging/azservicebus"
)

const leeway = 5 * time.Second

type Job struct {
	receiver *azservicebus.Receiver
	Message  *azservicebus.ReceivedMessage
}

func (j *Job) Complete(ctx context.Context) error {
	err := j.receiver.CompleteMessage(ctx, j.Message, nil)
	return err
}

func (j *Job) Abandon(ctx context.Context) error {
	err := j.receiver.AbandonMessage(ctx, j.Message, nil)
	return err
}

func (j *Job) Kill(ctx context.Context) error {
	err := j.receiver.DeadLetterMessage(ctx, j.Message, nil)
	return err
}

func (j *Job) RenewLock(ctx context.Context) error {
	err := j.receiver.RenewMessageLock(ctx, j.Message, nil)
	return err
}

func (j *Job) KeepAlive(ctx context.Context) error {
	err := j.RenewLock(ctx)
	if err != nil {
		return err
	}

	go func() {
		for {
			time.Sleep(j.Message.LockedUntil.Sub(time.Now()) - leeway)
			select {
			case <-ctx.Done():
				return
			default:
			}
			err := j.RenewLock(ctx)
			if err != nil {
				fmt.Println("Failed to renew lock")
				return
			}
		}
	}()

	return nil
}
