# pubsub-poc

## Example

```golang
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"cloud.google.com/go/pubsub"
	pubsubpoc "github.com/allisson/pubsub-poc"
	gocloudpubsub "gocloud.dev/pubsub"
	_ "gocloud.dev/pubsub/gcppubsub"
)

func publish(ctx context.Context, projectID, topicID string) {
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/topics/%s", projectID, topicID)
	producer, err := pubsubpoc.OpenProducer(ctx, driverURL)
	if err != nil {
		log.Fatal(err)
	}

	counter := 0
	for {
		counter++
		msg := &gocloudpubsub.Message{Body: []byte(fmt.Sprintf(`{"id": %d}`, counter))}
		producer.Send(ctx, msg)
		time.Sleep(5 * time.Second)
	}
}

func main() {
	// Use pubsub emulator
	// docker run --rm -p "8085:8085" allisson/gcloud-pubsub-emulator
	os.Setenv("PUBSUB_EMULATOR_HOST", "localhost:8085")

	// Fixtures
	ctx := context.Background()
	projectID := "my-project"
	topicID := "my-topic"
	subID := "my-subscription"
	consumerHandler := func(ctx context.Context, msg *gocloudpubsub.Message) error {
		time.Sleep(10 * time.Second)
		return nil
	}
	maxGoroutines := 10

	// Create topic
	topic, err := pubsubpoc.GCPCreateTopic(ctx, projectID, topicID)
	if err != nil {
		log.Fatal(err)
	}

	// Create subscription
	subConfig := pubsub.SubscriptionConfig{Topic: topic, AckDeadline: 60 * time.Second}
	_, err = pubsubpoc.GCPCreateSubscription(ctx, projectID, subID, subConfig)
	if err != nil {
		log.Fatal(err)
	}

	// Publish messages every 5 seconds
	go publish(ctx, projectID, topicID)

	// Open consumer
	driverURL := fmt.Sprintf("gcppubsub://projects/%s/subscriptions/%s", projectID, subID)
	consumer, err := pubsubpoc.OpenConsumer(ctx, driverURL, consumerHandler, maxGoroutines)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful shutdown
	idleChan := make(chan struct{})
	go func() {
		sigint := make(chan os.Signal, 1)

		// interrupt signal sent from terminal
		signal.Notify(sigint, os.Interrupt)
		// sigterm signal sent from kubernetes
		signal.Notify(sigint, syscall.SIGTERM)

		<-sigint

		// We received an interrupt signal, shut down.
		if err := consumer.Shutdown(ctx); err != nil {
			log.Printf("consumer_shutdown_error, err=%s\n", err.Error())
		}

		close(idleChan)
	}()

	// Start consumer
	if err := consumer.Start(ctx); err != nil {
		log.Printf("consumer_error, err=%s\n", err.Error())
	}

	<-idleChan

	log.Println("consumer_shutdown_completed")
}
```
