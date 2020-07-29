package pubsubpoc

import (
	"context"
	"fmt"

	"go.uber.org/zap"
	gcloudpubsub "gocloud.dev/pubsub"
)

// Producer allows you to send messages to a specific topic.
type Producer struct {
	projectID string
	topicID   string
	topic     *gcloudpubsub.Topic
}

// Send message to the topic.
func (p *Producer) Send(ctx context.Context, msg *gcloudpubsub.Message) error {
	if err := p.topic.Send(ctx, msg); err != nil {
		logger.Error(
			"producer_message_send_error",
			zap.String("project_id", p.projectID),
			zap.String("topic_id", p.topicID),
			zap.String("msg_body", string(msg.Body)),
			zap.Reflect("msg_metadata", msg.Metadata),
			zap.Error(err),
		)
		return err
	}

	logger.Info(
		"producer_message_sent",
		zap.String("project_id", p.projectID),
		zap.String("topic_id", p.topicID),
		zap.String("msg_body", string(msg.Body)),
		zap.Reflect("msg_metadata", msg.Metadata),
	)
	return nil
}

// Shutdown topic.
func (p *Producer) Shutdown(ctx context.Context) error {
	return p.topic.Shutdown(ctx)
}

// OpenProducer returns a new producer.
func OpenProducer(ctx context.Context, projectID, topicID string) (Producer, error) {
	producer := Producer{projectID: projectID, topicID: topicID}
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/topics/%s", projectID, topicID)

	topic, err := gcloudpubsub.OpenTopic(ctx, driverURL)
	if err != nil {
		return producer, err
	}

	producer.topic = topic
	return producer, nil
}
