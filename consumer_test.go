package pubsubpoc

import (
	"context"
	"os"
	"sync"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	gcloudpubsub "gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

func TestOpenConsumer(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Fixtures
	projectID := "my-project-id-3"
	topicID := "my-topic-id-3"
	subID := "my-sub-id-3"
	ctx := context.Background()
	counter := 0
	wg := sync.WaitGroup{}
	wg.Add(1)
	handler := func(ctx context.Context, msg *gcloudpubsub.Message) error {
		defer wg.Done()
		counter++
		return nil
	}
	maxGoroutines := 1

	// Create topic
	topic, err := CreateTopic(ctx, projectID, topicID)
	assert.Nil(t, err)
	assert.Equal(t, topicID, topic.ID())

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic}
	sub, err := CreateSubscription(ctx, projectID, subID, subConfig)
	assert.Nil(t, err)
	assert.Equal(t, subID, sub.ID())

	// Open producer
	producer, err := OpenProducer(ctx, projectID, topicID)
	assert.Nil(t, err)
	// nolint:errcheck
	defer producer.Shutdown(ctx)

	// Publish message
	msg := &gcloudpubsub.Message{
		Body: []byte("message-body"),
		Metadata: map[string]string{
			"attr1": "attr1",
			"attr2": "attr2",
		},
	}
	err = producer.Send(ctx, msg)
	assert.Nil(t, err)

	// Open consumer
	consumer, err := OpenConsumer(ctx, projectID, subID, handler, maxGoroutines)
	assert.Nil(t, err)
	// nolint:errcheck
	defer consumer.Shutdown(ctx)

	// Start consumer
	// nolint:errcheck
	go consumer.Start(ctx)

	// Wait for handle message
	wg.Wait()

	// Counter must be incremented once
	assert.Equal(t, 1, counter)
}
