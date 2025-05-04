package requests

import (
	"encoding/json"
)

type CreateAuctionRequest struct {
	RollupId           string `json:"rollupId"`
	SlotNumber         int64  `json:"slotNumber"`
	SlotTime           int    `json:"slotTime"`
	LeaderTxOrdererUrl string `json:"leaderTxOrdererUrl"`
}

func (r *CreateAuctionRequest) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
