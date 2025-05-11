package requests

import (
	"encoding/json"
)

type CreateAuctionRequest struct {
	RollupId              string `json:"rollupId"`
	SlotNumber            int64  `json:"slotNumber"`
	SlotTime              uint64 `json:"slotTime"`
	AuctionStartTimestamp uint64 `json:"auctionStartTimestamp"`
	LeaderTxOrdererUrl    string `json:"leaderTxOrdererUrl"`
}

func (r *CreateAuctionRequest) Marshal() ([]byte, error) {
	return json.Marshal(r)
}
