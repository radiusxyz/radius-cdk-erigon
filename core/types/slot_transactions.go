package types

type SlotTransactions struct {
	SlotNumber      int64    `json:"slotNumber"`
	RawTransactions [][]byte `json:"rawTransactions"`
}
