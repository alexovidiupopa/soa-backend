package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
)

type EmailRequest struct {
	To      string `json:"to"`
	Subject string `json:"subject"`
	Body    string `json:"body"`
}

func Handle(ctx context.Context, req []byte) ([]byte, error) {
	var msg EmailRequest
	if err := json.Unmarshal(req, &msg); err != nil {
		return nil, fmt.Errorf("invalid JSON: %v", err)
	}

	log.Printf("Sending email to %s\nSubject: %s\nBody: %s\n", msg.To, msg.Subject, msg.Body)
	return []byte(fmt.Sprintf("Email successfully sent to %s", msg.To)), nil
}
