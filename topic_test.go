package pubsubpoc

import (
	"context"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestTopic(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Pubsub client
	projectID := "my-project-id"
	client, err := pubsub.NewClient(context.Background(), projectID)
	assert.Nil(t, err)

	t.Run("Publish", func(t *testing.T) {
		ctx := context.Background()
		topicID := "topic-id-1"
		topic := NewTopic(topicID, client)

		// Create topic
		_, err := topic.Create(ctx)
		assert.Nil(t, err)

		// Publish to topic
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		id, err := topic.Publish(ctx, []byte(`{"payload": true}`), attributes)
		assert.Nil(t, err)
		assert.NotEqual(t, "", id)
	})

	t.Run("Publish with timeout", func(t *testing.T) {
		expectedError := "rpc error: code = DeadlineExceeded desc = context deadline exceeded"
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		defer cancel()
		topicID := "topic-id-1"
		topic := NewTopic(topicID, client)

		// Create topic
		_, err := topic.Create(context.Background())
		assert.Nil(t, err)

		// Publish to topic
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		_, err = topic.Publish(ctx, []byte(`{"payload": true}`), attributes)
		assert.Equal(t, expectedError, err.Error())
	})
}
