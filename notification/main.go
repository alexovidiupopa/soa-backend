package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
)

type NotificationEvent struct {
	Type    string `json:"type"`
	Email   string `json:"email"`
	Message string `json:"message"`
}

var rabbitURL string

func main() {
	rabbitURL = getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/")
	conn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("rabbit dial: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"email_notifications",
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("queue declare: %v", err)
	}

	msgs, err := ch.Consume(
		q.Name,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("notification service waiting for messages...")

	// Handle messages in background goroutine
	go func() {
		for d := range msgs {
			var ev map[string]interface{}
			if err := json.Unmarshal(d.Body, &ev); err != nil {
				log.Printf("invalid message: %v", err)
				_ = d.Nack(false, false) // discard invalid message
				continue
			}

			log.Printf("Received message for %s:", ev)

			// call OpenFaaS function
			if err := triggerEmailFaaS("data"); err != nil {
				log.Printf("error calling faas: %v", err)
				_ = d.Ack(false) // requeue for retry
				continue
			}

			_ = d.Ack(false)
		}
	}()

	// Wait for termination signal (Ctrl+C)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	<-sigs

	log.Println("shutting down notification service")
}

// getenv reads env var or returns default
func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

// triggerEmailFaaS calls the OpenFaaS send-email function
func triggerEmailFaaS(body string) error {
	gatewayURL := "http://host.docker.internal:8080/function/send-email"

	payload := map[string]string{
		//"to":      to,
		//"subject": subject,
		"body": body,
	}

	jsonBytes, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("failed to marshal email request: %w", err)
	}

	resp, err := http.Post(gatewayURL, "application/json", bytes.NewReader(jsonBytes))
	if err != nil {
		return fmt.Errorf("failed calling send-email function: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("send-email failed with status %d", resp.StatusCode)
	}

	return nil
}
