package pubsubpoc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func getTopic(ctx context.Context, client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
	// Get topic
	topic := client.Topic(topicID)
	ok, err := topic.Exists(ctx)
	if err != nil {
		return topic, err
	}
	if !ok {
		return topic, ErrTopicNotFound
	}
	return topic, nil
}

// TopicManager ...
type TopicManager interface {
	Create(ctx context.Context, topicID string) (*pubsub.Topic, error)
	Publish(ctx context.Context, topicID string, payload []byte, attributes map[string]string) (string, error)
}

type topicManager struct {
	client *pubsub.Client
}

func (t *topicManager) Create(ctx context.Context, topicID string) (*pubsub.Topic, error) {
	// Check if topic exists
	topic, err := getTopic(ctx, t.client, topicID)
	if err != ErrTopicNotFound {
		return topic, err
	}

	// Create topic
	topic, err = t.client.CreateTopic(ctx, topicID)
	if err != nil {
		logger.Error("topic_create", zap.String("topic_id", topicID), zap.Error(err))
	} else {
		logger.Info("topic_created", zap.String("topic_id", topicID))
	}
	return topic, err
}

func (t *topicManager) Publish(ctx context.Context, topicID string, payload []byte, attributes map[string]string) (string, error) {
	// Get topic
	topic, err := getTopic(ctx, t.client, topicID)
	if err != nil {
		return "", err
	}

	// Publish message
	message := &pubsub.Message{Data: payload, Attributes: attributes}
	result := topic.Publish(ctx, message)
	id, err := result.Get(ctx)
	if err != nil {
		logger.Error("topic_publish", zap.String("topic_id", topicID), zap.Error(err))
	} else {
		logger.Info("topic_published", zap.String("topic_id", topicID), zap.String("message_id", id))
	}
	return id, err
}

// NewTopicManager ...
func NewTopicManager(client *pubsub.Client) TopicManager {
	return &topicManager{client: client}
}
