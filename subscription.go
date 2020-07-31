package pubsubpoc

import (
	"context"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

// GCPCreateSubscription creates a new subscription on google cloud pubsub and deal if subscription already exists.
func GCPCreateSubscription(ctx context.Context, projectID, subID string, subConfig pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	client, err := pubsub.NewClient(context.Background(), projectID)
	if err != nil {
		return nil, err
	}
	defer client.Close()

	AlreadyExistsErr := "rpc error: code = AlreadyExists desc = Subscription already exists"
	sub, err := client.CreateSubscription(ctx, subID, subConfig)
	if err != nil {
		if err.Error() == AlreadyExistsErr {
			sub := client.Subscription(subID)
			return sub, nil
		}
		logger.Error("subscription_create", zap.String("subscription_id", subID), zap.Error(err))
		return sub, err
	}

	logger.Info("subscription_created", zap.String("subscription_id", subID))
	return sub, nil
}
