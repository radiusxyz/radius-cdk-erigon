package sbbservice

type GetRawTransactionsParams struct {
	RollupId   string `json:"rollupId"`
	Mode       string `json:"mode"`
	SlotNumber int64  `json:"slotNumber"`
}

type GetRawTransactionsResponse struct {
	RawTransactions []string `json:"rawTransactions"`
}

type JSONRPCRequest[T any] struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  []T    `json:"params"`
	ID      int    `json:"id"`
}

func newJsonRpcRequest[T any](method Method, params T) JSONRPCRequest[T] {
	return JSONRPCRequest[T]{
		JSONRPC: "2.0",
		Method:  string(method),
		Params:  []T{params},
		ID:      1,
	}
}
