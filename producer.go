package pubsubpoc

import (
	"context"

	"go.uber.org/zap"
	gocloudpubsub "gocloud.dev/pubsub"
)

// Producer allows you to send messages to a specific topic.
type Producer struct {
	driverURL string
	topic     *gocloudpubsub.Topic
}

// Send message to the topic.
func (p *Producer) Send(ctx context.Context, msg *gocloudpubsub.Message) error {
	if err := p.topic.Send(ctx, msg); err != nil {
		logger.Error(
			"producer_message_send_error",
			zap.String("driver_url", p.driverURL),
			zap.String("msg_body", string(msg.Body)),
			zap.Reflect("msg_metadata", msg.Metadata),
			zap.Error(err),
		)
		return err
	}

	logger.Info(
		"producer_message_sent",
		zap.String("driver_url", p.driverURL),
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
func OpenProducer(ctx context.Context, driverURL string) (Producer, error) {
	producer := Producer{driverURL: driverURL}

	topic, err := gocloudpubsub.OpenTopic(ctx, driverURL)
	if err != nil {
		return producer, err
	}

	producer.topic = topic
	return producer, nil
}
