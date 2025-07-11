package events

type AuctionClosedEvent struct {
	RollupId        *string  `json:"rollupId"`
	AuctionId       *string  `json:"auctionId"`
	SlotNumber      *int64   `json:"slotNumber"`
	RawTransactions [][]byte `json:"rawTransactions"`
}
