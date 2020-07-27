package pubsubpoc

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestSubscriptionManager(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Pubsub client
	projectID := "my-project-id"
	topicID := "my-topic-id-3"
	client, err := pubsub.NewClient(context.Background(), projectID)
	assert.Nil(t, err)

	// Create topic
	tm := NewTopicManager(client)
	topic, err := tm.Create(context.Background(), topicID)
	assert.Nil(t, err)

	t.Run("Create", func(t *testing.T) {
		ctx := context.Background()
		subID := "sub-id-1"
		subConfig := pubsub.SubscriptionConfig{}
		sm := NewSubscriptionManager(client)

		// Create a new subscription
		sub, err := sm.Create(ctx, topic.ID(), subID, subConfig)
		assert.Nil(t, err)
		assert.Equal(t, subID, sub.ID())

		// Create with subscription exists
		sub, err = sm.Create(ctx, topic.ID(), subID, subConfig)
		assert.Nil(t, err)
		assert.Equal(t, subID, sub.ID())
	})

	t.Run("Create with timeout", func(t *testing.T) {
		expectedError := "rpc error: code = DeadlineExceeded desc = context deadline exceeded"
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Microsecond)
		defer cancel()
		subID := "sub-id-1"
		subConfig := pubsub.SubscriptionConfig{}
		sm := NewSubscriptionManager(client)

		// Create a new subscription
		_, err := sm.Create(ctx, topic.ID(), subID, subConfig)
		assert.Equal(t, expectedError, err.Error())
	})
}

func TestSubscriptionHandlerManager(t *testing.T) {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Pubsub client
	projectID := "my-project-id"
	topicID := "my-topic-id-4"
	subID1 := "my-sub-1"
	subID2 := "my-sub-2"
	client, err := pubsub.NewClient(context.Background(), projectID)
	assert.Nil(t, err)

	// Create topic
	tm := NewTopicManager(client)
	topic, err := tm.Create(context.Background(), topicID)
	assert.Nil(t, err)

	// Create subscriptions
	subConfig := pubsub.SubscriptionConfig{}
	sm := NewSubscriptionManager(client)
	_, err = sm.Create(context.Background(), topic.ID(), subID1, subConfig)
	assert.Nil(t, err)
	_, err = sm.Create(context.Background(), topic.ID(), subID2, subConfig)
	assert.Nil(t, err)

	t.Run("Add", func(t *testing.T) {
		counter := 0
		wg := sync.WaitGroup{}
		wg.Add(2)
		ctx := context.Background()
		subHandler := func(ctx context.Context, msg *pubsub.Message) error {
			logger.Info("message_received", zap.String("message_id", msg.ID))
			counter++
			msg.Ack()
			logger.Info("message_acked", zap.String("message_id", msg.ID))
			wg.Done()
			return nil
		}
		shm := NewSubscriptionHandlerManager(client)
		err := shm.Add(ctx, subID1, subHandler, 1)
		assert.Nil(t, err)
		err = shm.Add(ctx, subID2, subHandler, 1)
		assert.Nil(t, err)
		defer shm.Stop(ctx)

		// Publish message
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		_, err = tm.Publish(ctx, topicID, []byte(`{"payload": true}`), attributes)
		assert.Nil(t, err)

		// SubscriptionHandlerManager run
		go func() {
			err := shm.Run(ctx)
			assert.Nil(t, err)
		}()

		// Wait for handler execution
		wg.Wait()

		// The counter must be incremented twice
		assert.Equal(t, 2, counter)
	})
}
