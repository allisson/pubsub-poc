# pubsub-poc

## Example

```golang
package main

import (
	"context"
	"log"
	"os"
	"time"

	"cloud.google.com/go/pubsub"
	pubsubpoc "github.com/allisson/pubsub-poc"
)

func publish(topic pubsubpoc.Topic) {
	// publish message every 30 seconds
	for {
		attributes := map[string]string{"attr1": "attr1", "attr2": "attr2"}
		_, err := topic.Publish(context.Background(), []byte(`{"payload": true}`), attributes)
		if err != nil {
			log.Fatal(err)
		}
		time.Sleep(30 * time.Second)
	}
}

func handler(ctx context.Context, msg *pubsub.Message) error {
	log.Printf("receive_message, id=%s, payload=%s\n", msg.ID, string(msg.Data))
	msg.Ack()
	return nil
}

func main() {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Data
	ctx := context.Background()
	projectID := "my-project"
	subID1 := "transactions-sub-1"
	subID2 := "transactions-sub-2"
	topicID := "transactions"
	subConfig := pubsub.SubscriptionConfig{}
	maxOutstandingMessages := uint(1)

	// Pubsub client
	client, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatal(err)
	}

	// Create topic
	topic := pubsubpoc.NewTopic(topicID, client)
	_, err = topic.Create(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Create subscriptions
	sub1 := pubsubpoc.NewSubscription(
		subID1,
		topicID,
		pubsub.SubscriptionConfig{},
		client,
	)
	_, err = sub1.Create(ctx)
	if err != nil {
		log.Fatal(err)
	}
	sub2 := pubsubpoc.NewSubscription(
		subID2,
		topicID,
		pubsub.SubscriptionConfig{},
		client,
	)
	_, err = sub2.Create(ctx)
	if err != nil {
		log.Fatal(err)
	}

	// Publish messages
	go publish(topic)

	// Create manager
	m := pubsubpoc.NewManager(client)
	m.AddConsumer(subID1, topicID, subConfig, handler, maxOutstandingMessages)
	m.AddConsumer(subID2, topicID, subConfig, handler, maxOutstandingMessages)

	// Run manager
	m.Run()
}
```