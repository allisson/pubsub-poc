package pubsubpoc

import (
	"context"
	"sync"

	"cloud.google.com/go/pubsub"
	"go.uber.org/zap"
)

// Manager ...
type Manager interface {
	AddConsumer(subID, topicID string, subConfig pubsub.SubscriptionConfig, fn ConsumeFunc, maxOutstandingMessages uint)
	Run()
	Stop()
}

type consumer struct {
	subscription           Subscription
	consumeFunc            ConsumeFunc
	maxOutstandingMessages uint
}

type manager struct {
	client             *pubsub.Client
	consumers          []consumer
	mux                sync.Mutex
	idleConsumerClosed chan struct{}
}

func (m *manager) AddConsumer(subID, topicID string, subConfig pubsub.SubscriptionConfig, fn ConsumeFunc, maxOutstandingMessages uint) {
	m.mux.Lock()
	defer m.mux.Unlock()
	sub := NewSubscription(subID, topicID, subConfig, m.client)
	c := consumer{
		subscription:           sub,
		consumeFunc:            fn,
		maxOutstandingMessages: maxOutstandingMessages,
	}
	m.consumers = append(m.consumers, c)
}

func (m *manager) Run() {
	logger.Info("manager_run_started")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start consumers
	for _, c := range m.consumers {
		go func(ctx context.Context, c consumer) {
			if err := c.subscription.Consume(ctx, c.consumeFunc, c.maxOutstandingMessages); err != nil {
				logger.Error("", zap.Error(err))
			}
		}(ctx, c)
	}

	<-m.idleConsumerClosed

	logger.Info("manager_run_finished")
}

func (m *manager) Stop() {
	logger.Info("manager_stop_started")
	close(m.idleConsumerClosed)
	logger.Info("manager_stop_finished")
}

// NewManager ...
func NewManager(client *pubsub.Client) Manager {
	return &manager{
		client:             client,
		idleConsumerClosed: make(chan struct{}),
	}
}
