package events

import (
	"encoding/json"
)

type EventType string

const (
	AuctionClosed EventType = "AuctionClosed"
)

type EventMessage struct {
	EventType EventType       `json:"eventType"`
	Payload   json.RawMessage `json:"payload"`
}

func (e *EventMessage) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
