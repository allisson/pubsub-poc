package pubsubpoc

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	gcloudpubsub "gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

func TestOpenProducer(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Fixtures
	projectID := "my-project-id-2"
	topicID := "my-topic-id-2"
	ctx := context.Background()

	// Create topic
	topic, err := CreateTopic(ctx, projectID, topicID)
	assert.Nil(t, err)
	assert.Equal(t, topicID, topic.ID())

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
}
