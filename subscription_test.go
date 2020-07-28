package pubsubpoc

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestSubscription(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Context
	ctx := context.Background()
	subID := "my-sub-id-1"
	topicID := "my-topic-id-1"
	counter := 0

	// Pubsub client
	projectID := "my-project-id"
	client, err := pubsub.NewClient(context.Background(), projectID)
	assert.Nil(t, err)

	// Consume handler
	consumerHandler := func(ctx context.Context, msg *pubsub.Message) error {
		counter++
		return nil
	}

	// Create topic
	topic := NewTopic(topicID, client)
	_, err = topic.Create(ctx)
	assert.Nil(t, err)

	// Create subscription
	sub := NewSubscription(
		subID,
		topicID,
		pubsub.SubscriptionConfig{},
		client,
	)
	_, err = sub.Create(ctx)
	assert.Nil(t, err)

	// Publish message
	attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
	_, err = topic.Publish(ctx, []byte(`{"payload": true}`), attributes)
	assert.Nil(t, err)

	// Run consume
	// nolint:errcheck
	go sub.Consume(ctx, consumerHandler, 1)

	// Wait to consume message
	time.Sleep(100 * time.Millisecond)

	// The counter must be incremented once
	assert.True(t, counter > 0)
}
