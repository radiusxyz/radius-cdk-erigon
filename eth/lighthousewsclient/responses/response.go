package responses

import (
	"encoding/json"
)

const (
	SUCCESS Status = iota
	FAILURE
)

type Status int

type ResponseType string

const (
	RollupVerified ResponseType = "RollupVerified"
	AuctionCreated ResponseType = "AuctionCreated"
)

type ResponseMessage struct {
	Id           string          `json:"id"`
	ResponseType ResponseType    `json:"responseType"`
	Status       int             `json:"status"`
	Payload      json.RawMessage `json:"payload"`
	Error        *ErrorMessage   `json:"error"`
}

type ErrorMessage struct {
	Message string `json:"message"`
}

func (m *ResponseMessage) Marshal() ([]byte, error) {
	return json.Marshal(m)
}
