package pubsubpoc

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
)

func TestCreateSubscription(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Fixtures
	projectID := "my-project-id-1"
	topicID := "my-topic-id-1"
	subID := "my-sub-id-1"
	ctx := context.Background()

	// Create topic
	topic, err := CreateTopic(ctx, projectID, topicID)
	assert.Nil(t, err)
	assert.Equal(t, topicID, topic.ID())

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic}
	sub, err := CreateSubscription(ctx, projectID, subID, subConfig)
	assert.Nil(t, err)
	assert.Equal(t, subID, sub.ID())

	// Create again to force already exists error
	sub, err = CreateSubscription(ctx, projectID, subID, subConfig)
	assert.Nil(t, err)
	assert.Equal(t, subID, sub.ID())
}
