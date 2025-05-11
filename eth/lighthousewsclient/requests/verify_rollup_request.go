package requests

import (
	"encoding/json"
)

type VerifyRollupRequest struct {
	RollupId  string `json:"rollupId"`
	Timestamp uint64 `json:"timestamp"`
	Signature []byte `json:"signature"`
}

func (r *VerifyRollupRequest) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
