package pubsubpoc

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

func getSubscription(ctx context.Context, client *pubsub.Client, subID string) (*pubsub.Subscription, error) {
	// Get subscription
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

// SubscriptionManager ...
type SubscriptionManager interface {
	Create(ctx context.Context, topicID, subID string, subConfig pubsub.SubscriptionConfig) (*pubsub.Subscription, error)
}

type subscriptionManager struct {
	client *pubsub.Client
}

func (s *subscriptionManager) Create(ctx context.Context, topicID, subID string, subConfig pubsub.SubscriptionConfig) (*pubsub.Subscription, error) {
	// Get topic
	topic, err := getTopic(ctx, s.client, topicID)
	if err != nil {
		return &pubsub.Subscription{}, err
	}

	// Check if subscription exists
	sub, err := getSubscription(ctx, s.client, subID)
	if err != ErrSubscriptionNotFound {
		return sub, err
	}

	// Create subscription
	subConfig.Topic = topic
	sub, err = s.client.CreateSubscription(ctx, subID, subConfig)
	if err != nil {
		logger.Error("subscription_create", zap.String("subscription_id", subID), zap.Error(err))
	} else {
		logger.Info("subscription_created", zap.String("subscription_id", subID))
	}
	return sub, err
}

// NewSubscriptionManager ...
func NewSubscriptionManager(client *pubsub.Client) SubscriptionManager {
	return &subscriptionManager{client: client}
}

// SubscriptionHandler ...
type SubscriptionHandler func(ctx context.Context, msg *pubsub.Message) error

// SubscriptionHandlerManager ...
type SubscriptionHandlerManager interface {
	Add(ctx context.Context, subID string, handler SubscriptionHandler, maxOutstandingMessages uint) error
	Run(ctx context.Context) error
	Stop(ctx context.Context) error
}

type handlerItem struct {
	Subscription *pubsub.Subscription
	Handler      SubscriptionHandler
}

type subscriptionHandlerManager struct {
	client            *pubsub.Client
	handlers          []*handlerItem
	mux               sync.Mutex
	idleHandlerClosed chan struct{}
}

func (s *subscriptionHandlerManager) Add(ctx context.Context, subID string, handler SubscriptionHandler, maxOutstandingMessages uint) error {
	s.mux.Lock()
	sub, err := getSubscription(ctx, s.client, subID)
	if err != nil {
		return err
	}
	sub.ReceiveSettings.MaxOutstandingMessages = int(maxOutstandingMessages)
	s.handlers = append(s.handlers, &handlerItem{Subscription: sub, Handler: handler})
	s.mux.Unlock()
	return nil
}

func (s *subscriptionHandlerManager) Stop(ctx context.Context) error {
	logger.Info("subscription_handler_manager_stop")
	close(s.idleHandlerClosed)
	return nil
}

func (s *subscriptionHandlerManager) Run(ctx context.Context) error {
	logger.Info("subscription_handler_manager_run")
	newCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Execute handlers
	for _, item := range s.handlers {
		go func(i *handlerItem) {
			subID := i.Subscription.ID()
			logger.Info("subscription_receive_started", zap.String("subscription_id", subID))

			err := i.Subscription.Receive(newCtx, func(ctx context.Context, msg *pubsub.Message) {
				logger.Info("subscription_receive_handle_started", zap.String("subscription_id", subID), zap.Reflect("message", msg))
				if err := i.Handler(ctx, msg); err != nil {
					logger.Error("subscription_receive_handle_error", zap.String("subscription_id", subID), zap.Reflect("message", msg), zap.Error(err))
					msg.Nack()
					return
				}
				msg.Ack()
				logger.Info("subscription_receive_handle_completed", zap.String("subscription_id", subID), zap.Reflect("message", msg))
			})

			if err != context.Canceled {
				logger.Error("subscription_receive_error", zap.String("subscription_id", i.Subscription.ID()), zap.Error(err))
			}
		}(item)
	}

	<-s.idleHandlerClosed

	return nil
}

// NewSubscriptionHandlerManager ...
func NewSubscriptionHandlerManager(client *pubsub.Client) SubscriptionHandlerManager {
	return &subscriptionHandlerManager{
		client:            client,
		idleHandlerClosed: make(chan struct{}),
	}
}
