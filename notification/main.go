package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

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

	q, err := ch.QueueDeclare("email_notifications", true, false, false, false, nil)
	if err != nil {
		log.Fatalf("queue declare: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		log.Fatalf("consume: %v", err)
	}

	log.Println("notification service waiting for messages...")
	for d := range msgs {
		var ev map[string]interface{}
		_ = json.Unmarshal(d.Body, &ev)
		// simulate sending email
		log.Printf("NOTIFICATION: got event -> %v", ev)
		// ack
		_ = d.Ack(false)
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func triggerFaas(event NotificationEvent) error {
	// Local OpenFaaS gateway
	gateway := "http://gateway:8080" // if running inside docker-compose

	function := "send-email" // replace with your function name
	url := fmt.Sprintf("%s/function/%s", gateway, function)

	payload, _ := json.Marshal(event)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(payload))
	if err != nil {
		return fmt.Errorf("failed to invoke OpenFaaS function: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("faas function returned %s", resp.Status)
	}

	log.Printf("invoked OpenFaaS function '%s' for %s", function, event.Email)
	return nil
}
