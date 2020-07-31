package pubsubpoc

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	gocloudpubsub "gocloud.dev/pubsub"
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
	topic, err := GCPCreateTopic(ctx, projectID, topicID)
	assert.Nil(t, err)
	assert.Equal(t, topicID, topic.ID())

	// Open producer
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/topics/%s", projectID, topicID)
	producer, err := OpenProducer(ctx, driverURL)
	assert.Nil(t, err)
	// nolint:errcheck
	defer producer.Shutdown(ctx)

	// Publish message
	msg := &gocloudpubsub.Message{
		Body: []byte("message-body"),
		Metadata: map[string]string{
			"attr1": "attr1",
			"attr2": "attr2",
		},
	}
	err = producer.Send(ctx, msg)
	assert.Nil(t, err)
}
