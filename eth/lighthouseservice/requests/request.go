package requests

import (
	"encoding/json"
)

type RequestType string

var (
	VerifyRollup  RequestType = "VerifyRollup"
	CreateAuction RequestType = "CreateAuction"
)

type RequestParams interface {
	Marshal() ([]byte, error)
}

type RequestMessage struct {
	Id          string          `json:"id"`
	RequestType RequestType     `json:"requestType"`
	Payload     json.RawMessage `json:"payload"`
}

func (r *RequestMessage) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
