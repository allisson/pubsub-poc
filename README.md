# pubsub-poc

## Example

```golang
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"

	"cloud.google.com/go/pubsub"
	pubsubpoc "github.com/allisson/pubsub-poc"
	gocloudpubsub "gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

func main() {
	// Use pubsub emulator
	// docker run --rm -p "8085:8085" allisson/gcloud-pubsub-emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Fixtures
	ctx := context.Background()
	projectID := "my-project"
	topicID := "my-topic"
	subID := "my-subscription"
	counter := 0
	wg := sync.WaitGroup{}
	wg.Add(1)
	consumerHandler := func(ctx context.Context, msg *gocloudpubsub.Message) error {
		defer wg.Done()
		counter++
		return nil
	}
	maxGoroutines := 1

	// Create topic
	topic, err := pubsubpoc.GCPCreateTopic(ctx, projectID, topicID)
	if err != nil {
		log.Fatal(err)
	}

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic}
	sub, err := pubsubpoc.GCPCreateSubscription(ctx, projectID, subID, subConfig)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("topic_created=%s, subscription_created=%s\n", topic.ID(), sub.ID())

	// Open producer
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/topics/%s", projectID, topicID)
	producer, err := pubsubpoc.OpenProducer(ctx, driverURL)
	if err != nil {
		log.Fatal(err)
	}
	// nolint:errcheck
	defer producer.Shutdown(ctx)

	// Publish message
	msg := &gocloudpubsub.Message{
		Body: []byte("message-body"),
		Metadata: map[string]string{
			"attr1": "attr1",
			"attr2": "attr2",
		},
	}
	err = producer.Send(ctx, msg)
	if err != nil {
		log.Fatal(err)
	}

	// Open consumer
	driverURL = fmt.Sprintf("gcppubsub://projects/%s/subscriptions/%s", projectID, subID)
	consumer, err := pubsubpoc.OpenConsumer(ctx, driverURL, consumerHandler, maxGoroutines)
	if err != nil {
		log.Fatal(err)
	}
	// nolint:errcheck
	defer consumer.Shutdown(ctx)

	// Start consumer
	// nolint:errcheck
	go consumer.Start(ctx)

	// Wait for handle message
	wg.Wait()

	// Counter must be incremented once
	fmt.Printf("counter=%d\n", counter)
}
```
