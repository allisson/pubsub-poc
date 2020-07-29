# pubsub-poc

## Example

```golang
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"cloud.google.com/go/pubsub"
	pubsubpoc "github.com/allisson/pubsub-poc"
)

func main() {
	// Use pubsub emulator
	// docker run --rm -p "8085:8085" allisson/gcloud-pubsub-emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Data
	ctx := context.Background()
	projectID := "my-project"
	topicID := "my-topic"
	subID := "my-subscription"

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
}
```
