package responses

type AuctionCreatedResponse struct {
	AuctionId          *string `json:"auctionId"`
	RollupId           *string `json:"rollupId"`
	SlotNumber         *int64  `json:"slotNumber"`
	SlotTime           *int    `json:"slotTime"`
	LeaderTxOrdererUrl *string `json:"leaderTxOrdererUrl"`
}
