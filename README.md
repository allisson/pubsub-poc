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
	gcloudpubsub "gocloud.dev/pubsub"
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
	consumerHandler := func(ctx context.Context, msg *gcloudpubsub.Message) error {
		defer wg.Done()
		counter++
		return nil
	}

	// Create topic
	topic, err := pubsubpoc.CreateTopic(ctx, projectID, topicID)
	if err != nil {
		log.Fatal(err)
	}

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic}
	sub, err := pubsubpoc.CreateSubscription(ctx, projectID, subID, subConfig)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("topic_created=%s, subscription_created=%s\n", topic.ID(), sub.ID())

	// Open producer
	producer, err := pubsubpoc.OpenProducer(ctx, projectID, topicID)
	if err != nil {
		log.Fatal(err)
	}
	// nolint:errcheck
	defer producer.Shutdown(ctx)

	// Publish message
	msg := &gcloudpubsub.Message{
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
	consumer, err := pubsubpoc.OpenConsumer(ctx, projectID, subID, consumerHandler)
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
