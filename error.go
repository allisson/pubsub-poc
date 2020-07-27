package pubsubpoc

import "errors"

var (
	// ErrTopicNotFound ...
	ErrTopicNotFound = errors.New("topic_not_found")
	// ErrSubscriptionNotFound ...
	ErrSubscriptionNotFound = errors.New("subscription_not_found")
)
