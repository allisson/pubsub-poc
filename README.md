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

func main() {
	// Use pubsub emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Data
	ctx := context.Background()
	projectID := "my-project"
	topicID := "my-topic"

	// Create topic
	topic, err := CreateTopic(ctx, projectID, topicID)
	if err != nil {
		log.Fatal(err)
	}

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic}
	sub, err := CreateSubscription(ctx, projectID, subID, subConfig)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("topic_created=%s, subscription_created=%s\n", topic.ID(), sub.ID())
}
```