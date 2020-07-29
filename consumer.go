package pubsubpoc

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	gcloudpubsub "gocloud.dev/pubsub"
)

// Handler represents a function to be passed to consumer.
type Handler func(ctx context.Context, msg *gcloudpubsub.Message) error

// Consumer allows you to consume message from a specific subscription.
type Consumer struct {
	projectID     string
	subID         string
	sub           *gcloudpubsub.Subscription
	fn            Handler
	maxGoroutines int
}

// Start message consumption.
func (c *Consumer) Start(ctx context.Context) error {
	// Code from https://gocloud.dev/howto/pubsub/subscribe/#receiving
	sem := make(chan struct{}, c.maxGoroutines)
recvLoop:
	for {
		msg, err := c.sub.Receive(ctx)
		if err != nil {
			// Errors from Receive indicate that Receive will no longer succeed.
			logger.Error(
				"consumer_receiving_error",
				zap.String("project_id", c.projectID),
				zap.String("subscription_id", c.subID),
				zap.Error(err),
			)
			break
		}

		// Wait if there are too many active handle goroutines and acquire the semaphore.
		// If the context is canceled, stop waiting and start shutting down.
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
			break recvLoop
		}

		// Handle the message in a new goroutine.
		go func() {
			// Release the semaphore.
			defer func() { <-sem }()

			// Execute handler
			if err := c.fn(ctx, msg); err != nil {
				logger.Error(
					"consumer_message_handle_error",
					zap.String("project_id", c.projectID),
					zap.String("subscription_id", c.subID),
					zap.String("msg_body", string(msg.Body)),
					zap.Reflect("msg_metadata", msg.Metadata),
					zap.Error(err),
				)
				msg.Nack()
				return
			}

			// Ack message
			msg.Ack()
			logger.Info(
				"consumer_message_handled",
				zap.String("project_id", c.projectID),
				zap.String("subscription_id", c.subID),
				zap.String("msg_body", string(msg.Body)),
				zap.Reflect("msg_metadata", msg.Metadata),
			)
		}()
	}

	// We're no longer receiving messages. Wait to finish handling any
	// unacknowledged messages by totally acquiring the semaphore.
	for n := 0; n < c.maxGoroutines; n++ {
		sem <- struct{}{}
	}

	return nil
}

// Shutdown subscription.
func (c *Consumer) Shutdown(ctx context.Context) error {
	return c.sub.Shutdown(ctx)
}

// OpenConsumer returns a new consumer.
func OpenConsumer(ctx context.Context, projectID, subID string, fn Handler, maxGoroutines int) (Consumer, error) {
	consumer := Consumer{projectID: projectID, subID: subID, fn: fn, maxGoroutines: maxGoroutines}
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/subscriptions/%s", projectID, subID)

	sub, err := gcloudpubsub.OpenSubscription(ctx, driverURL)
	if err != nil {
		return consumer, err
	}

	consumer.sub = sub
	return consumer, nil
}
