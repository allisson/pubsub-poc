package pubsubpoc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func getTopic(ctx context.Context, client *pubsub.Client, topicID string) (*pubsub.Topic, error) {
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

// Topic ...
type Topic interface {
	Create(ctx context.Context) (*pubsub.Topic, error)
	Publish(ctx context.Context, payload []byte, attributes map[string]string) (string, error)
}

type topic struct {
	id     string
	client *pubsub.Client
}

func (t *topic) Create(ctx context.Context) (*pubsub.Topic, error) {
	AlreadyExistsErr := "rpc error: code = AlreadyExists desc = Topic already exists"
	topic, err := t.client.CreateTopic(ctx, t.id)
	if err != nil {
		if err.Error() != AlreadyExistsErr {
			logger.Error("topic_create", zap.String("topic_id", t.id), zap.Error(err))
			return topic, err
		}
	}

	logger.Info("topic_created", zap.String("topic_id", t.id))
	return topic, nil
}

func (t *topic) Publish(ctx context.Context, payload []byte, attributes map[string]string) (string, error) {
	// Get topic
	topic, err := getTopic(ctx, t.client, t.id)
	if err != nil {
		return "", err
	}

	// Publish message
	message := &pubsub.Message{Data: payload, Attributes: attributes}
	result := topic.Publish(ctx, message)
	msgID, err := result.Get(ctx)
	if err != nil {
		logger.Error("topic_publish", zap.String("topic_id", t.id), zap.Error(err))
	} else {
		logger.Info("topic_published", zap.String("topic_id", t.id), zap.String("message_id", msgID))
	}
	return msgID, err
}

// NewTopic ...
func NewTopic(id string, client *pubsub.Client) Topic {
	return &topic{id: id, client: client}
}
