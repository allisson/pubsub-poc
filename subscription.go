package pubsubpoc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func getSubscription(ctx context.Context, client *pubsub.Client, subID, topicID string, subConfig pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	// Check if subscription exists
	sub := client.Subscription(subID)
	ok, err := sub.Exists(ctx)
	if err != nil {
		return sub, err
	}
	if !ok {
		return sub, ErrSubscriptionNotFound
	}

	return sub, nil
}

// ConsumeFunc ...
type ConsumeFunc func(ctx context.Context, msg *pubsub.Message) error

// Subscription ...
type Subscription interface {
	Create(ctx context.Context) (*pubsub.Subscription, error)
	Consume(ctx context.Context, fn ConsumeFunc, maxOutstandingMessages uint) error
}

type subscription struct {
	id        string
	topicID   string
	subConfig pubsub.SubscriptionConfig
	client    *pubsub.Client
}

func (s *subscription) Create(ctx context.Context) (*pubsub.Subscription, error) {
	// Get topic
	topic, err := getTopic(ctx, s.client, s.topicID)
	if err != nil {
		return &pubsub.Subscription{}, err
	}

	// Create subscription
	s.subConfig.Topic = topic
	AlreadyExistsErr := "rpc error: code = AlreadyExists desc = Subscription already exists"
	sub, err := s.client.CreateSubscription(ctx, s.id, s.subConfig)
	if err != nil {
		if err.Error() != AlreadyExistsErr {
			logger.Error("subscription_create", zap.String("subscription_id", s.id), zap.Error(err))
			return sub, err
		}
	}

	logger.Info("subscription_created", zap.String("subscription_id", s.id))
	return sub, nil
}

func (s *subscription) Consume(ctx context.Context, fn ConsumeFunc, maxOutstandingMessages uint) error {
	// Get subscription
	sub, err := getSubscription(ctx, s.client, s.id, s.topicID, s.subConfig)
	if err != nil {
		return err
	}

	logger.Info(
		"subscription_consume_started",
		zap.String("subscription_id", s.id),
		zap.String("topic_id", s.topicID),
		zap.Uint("max_outstanding_messages", maxOutstandingMessages),
	)

	err = sub.Receive(ctx, func(ctx context.Context, msg *pubsub.Message) {
		logger.Info(
			"subscription_consume_handle_message_started",
			zap.String("subscription_id", s.id),
			zap.String("topic_id", s.topicID),
			zap.Uint("max_outstanding_messages", maxOutstandingMessages),
			zap.String("message_id", msg.ID),
		)

		if err := fn(ctx, msg); err != nil {
			logger.Error(
				"subscription_consume_handle_message_error",
				zap.String("subscription_id", s.id),
				zap.String("topic_id", s.topicID),
				zap.Uint("max_outstanding_messages", maxOutstandingMessages),
				zap.String("message_id", msg.ID),
				zap.Reflect("message", msg),
				zap.Error(err),
			)
			msg.Nack()
			return
		}

		msg.Ack()
		logger.Info(
			"subscription_consume_handle_message_completed",
			zap.String("subscription_id", s.id),
			zap.String("topic_id", s.topicID),
			zap.Uint("max_outstanding_messages", maxOutstandingMessages),
			zap.String("message_id", msg.ID),
		)
	})

	if err != context.Canceled {
		logger.Error(
			"subscription_consume_started",
			zap.String("subscription_id", s.id),
			zap.String("topic_id", s.topicID),
			zap.Uint("max_outstanding_messages", maxOutstandingMessages),
			zap.Error(err),
		)
		return err
	}

	return nil
}

// NewSubscription ...
func NewSubscription(id, topicID string, subConfig pubsub.SubscriptionConfig, client *pubsub.Client) Subscription {
	return &subscription{
		id:        id,
		topicID:   topicID,
		subConfig: subConfig,
		client:    client,
	}
}
