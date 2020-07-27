package pubsubpoc

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestTopicManager(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Pubsub client
	projectID := "my-project-id"
	client, err := pubsub.NewClient(context.Background(), projectID)
	assert.Nil(t, err)

	t.Run("Create", func(t *testing.T) {
		ctx := context.Background()
		topicID := "topic-id-1"
		tm := NewTopicManager(client)

		// Create a new topic
		topic, err := tm.Create(ctx, topicID)
		assert.Nil(t, err)
		assert.Equal(t, topicID, topic.ID())

		// Create with topic exists
		topic, err = tm.Create(ctx, topicID)
		assert.Nil(t, err)
		assert.Equal(t, topicID, topic.ID())
	})

	t.Run("Create with timeout", func(t *testing.T) {
		expectedError := "rpc error: code = DeadlineExceeded desc = context deadline exceeded"
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		defer cancel()
		topicID := "topic-id-1"
		tm := NewTopicManager(client)

		// Create a new topic
		_, err := tm.Create(ctx, topicID)
		assert.Equal(t, expectedError, err.Error())
	})

	t.Run("Publish", func(t *testing.T) {
		ctx := context.Background()
		topicID := "topic-id-2"
		tm := NewTopicManager(client)

		// Create a new topic
		topic, err := tm.Create(ctx, topicID)
		assert.Nil(t, err)
		assert.Equal(t, topicID, topic.ID())

		// Publish to topic
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		id, err := tm.Publish(ctx, topicID, []byte(`{"payload": true}`), attributes)
		assert.Nil(t, err)
		assert.NotEqual(t, "", id)
	})

	t.Run("Publish with timeout", func(t *testing.T) {
		expectedError := "rpc error: code = DeadlineExceeded desc = context deadline exceeded"
		ctx := context.Background()
		topicID := "topic-id-2"
		tm := NewTopicManager(client)

		// Create a new topic
		topic, err := tm.Create(ctx, topicID)
		assert.Nil(t, err)
		assert.Equal(t, topicID, topic.ID())

		// Publish to topic
		newCtx, cancel := context.WithTimeout(ctx, 1*time.Microsecond)
		defer cancel()
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		_, err = tm.Publish(newCtx, topicID, []byte(`{"payload": true}`), attributes)
		assert.Equal(t, expectedError, err.Error())
	})
}
