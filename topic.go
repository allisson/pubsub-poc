package pubsubpoc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

// GCPCreateTopic creates a new topic on google cloud pubsub and deal if topic already exists.
func GCPCreateTopic(ctx context.Context, projectID, topicID string) (*pubsub.Topic, error) {
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	AlreadyExistsErr := "rpc error: code = AlreadyExists desc = Topic already exists"
	topic, err := client.CreateTopic(ctx, topicID)
	if err != nil {
		if err.Error() == AlreadyExistsErr {
			topic := client.Topic(topicID)
			return topic, nil
		}
		logger.Error("topic_create", zap.String("topic_id", topicID), zap.Error(err))
		return topic, err
	}

	logger.Info("topic_created", zap.String("topic_id", topicID))
	return topic, nil
}
