package sbbclient

type JSONRPCRequest[T any] struct {
	JSONRPC string `json:"jsonrpc"`
	Method  string `json:"method"`
	Params  T      `json:"params"`
	ID      int    `json:"id"`
}

func newJsonRpcRequest[T any](method Method, params T) JSONRPCRequest[T] {
	return JSONRPCRequest[T]{
		JSONRPC: "2.0",
		Method:  string(method),
		Params:  params,
		ID:      1,
	}
}

type GetTxOrdererRpcUrlsParams struct {
	TxOrdererAddresses []string `json:"sequencer_address_list"`
}

type TxOrdererRpcUrl struct {
	Address        string `json:"address"`
	ExternalRpcUrl string `json:"external_rpc_url"`
	ClusterRpcUrl  string `json:"cluster_rpc_url"`
}

type GetTxOrdererRpcUrlsResponse struct {
	TxOrdererRpcUrls []TxOrdererRpcUrl `json:"sequencer_rpc_url_list"`
}

type FinalizeBlockMessageParams struct {
	RollupId        string `json:"rollup_id"`
	ExecutorAddress string `json:"executor_address"`

	PlatformBlockHeight uint64 `json:"platform_block_height"`
	RollupBlockHeight   uint64 `json:"rollup_block_height"`

	BlockCreatorAddress     string `json:"block_creator_address"`
	NextBlockCreatorAddress string `json:"next_block_creator_address"`
}

type FinalizeBlockParams struct {
	Message   FinalizeBlockMessageParams `json:"finalize_block_message"`
	Signature string                     `json:"signature"`
}

type LeaderChangeMessage struct {
	RollupId                      string `json:"rollup_id"`
	ExecutorAddress               string `json:"executor_address"`
	CurrentLeaderTxOrdererAddress string `json:"current_leader_tx_orderer_address"`
	NextLeaderTxOrdererAddress    string `json:"next_leader_tx_orderer_address"`
	PlatformBlockHeight           uint64 `json:"platform_block_height"`
}

type GetRawTransactionsParams struct {
	LeaderChangeMessage LeaderChangeMessage `json:"leader_change_message"`
	RollupSignature     string              `json:"rollup_signature"`
}

type GetRawTransactionsResponse struct {
	RawTransactions []string `json:"raw_transaction_list"`
}

var abiString string = `[
		{
      "inputs": [
        {
          "internalType": "string",
          "name": "clusterId",
          "type": "string"
        }
      ],
      "name": "getSequencers",
      "outputs": [
        {
          "internalType": "address[]",
          "name": "",
          "type": "address[]"
        }
      ],
      "stateMutability": "view",
      "type": "function"
    }
	]`
