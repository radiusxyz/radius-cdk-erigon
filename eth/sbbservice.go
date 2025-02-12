package eth

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/iden3/go-iden3-crypto/keccak256"
	ethereum "github.com/ledgerwatch/erigon"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/sbbclient"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"math/big"
	"strings"
	"time"
)

type Method string

const (
	FinalizeBlock          Method = "finalize_block"
	GetRawTransactionList  Method = "get_raw_transaction_list"
	GetSequencerRpcUrlList Method = "get_sequencer_rpc_url_list"
)

type BlockchainService interface {
	SubmitRawTransactions(ctx context.Context, encodedTx [][]byte) error
	GetBlockNumber() (*uint64, error)
	BlockCreationCh() chan struct{}
	Config() *ethconfig.Config
}

type BlockTransactions struct {
	blockNumber  uint64
	transactions [][]byte
}

type SbbService struct {
	sbbClient           *sbbclient.SbbClient
	ethClient           *ethclient.Client
	blockchainService   BlockchainService
	sequencerPrivateKey *ecdsa.PrivateKey
	blockTransactionsCh chan *BlockTransactions
	sbbCtx              context.Context
}

func NewSbbService(ctx context.Context, blockchainService BlockchainService) (*SbbService, error) {
	sbbClient := sbbclient.New()
	ethClient, _ := ethclient.Dial(blockchainService.Config().PlatformUrl) // TODO: error handling
	sequencerPrivateKey, err := NewKeyFromKeystore(blockchainService.Config().SequencerPrivateKeyKeystorePassword)
	if err != nil {
		log.Fatal(err)
	}
	return &SbbService{
		sbbClient:           sbbClient,
		ethClient:           ethClient,
		blockchainService:   blockchainService,
		sequencerPrivateKey: sequencerPrivateKey,
		blockTransactionsCh: make(chan *BlockTransactions, blockchainService.Config().MaxSbbFinalizationCapacity),
		sbbCtx:              ctx,
	}, nil
}

func (s *SbbService) Start() {
	go s.requestToSbb()
	go s.executeSbbBlockTransactions()
}

func (s *SbbService) executeSbbBlockTransactions() {
	for {
		select {
		case blockTransactions := <-s.blockTransactionsCh:
			log.Info("SBB block transactions execution started.")
			currentBlockNumber, err := s.blockchainService.GetBlockNumber()
			if err != nil {
				panic("youngmin - currentBlock" + err.Error())
			}
			for blockTransactions.blockNumber != *currentBlockNumber+1 {
				time.Sleep(100 * time.Millisecond)
			}
			if err = s.blockchainService.SubmitRawTransactions(s.sbbCtx, blockTransactions.transactions); err != nil {
				panic("youngmin - SubmitRawTransactions" + err.Error())
			}
			s.blockchainService.BlockCreationCh() <- struct{}{}
		case <-s.sbbCtx.Done():
			return
		}
	}
}

func (s *SbbService) requestToSbb() {
	loopTime := int64(3000)
	timer := time.NewTimer(time.Duration(loopTime) * time.Millisecond)
	blockNumber, err := s.blockchainService.GetBlockNumber()
	if err != nil {
		panic(err.Error())
	}
	finalizedBlockNumber := *blockNumber

	var platformBlockNumber *uint64
	var validSequencerAddresses []string
	var sequencerRpcUrls []string
	var leaderSequencerIndex *uint64

	for {
		select {
		case <-timer.C:
			startTime := time.Now().UnixMilli()

			Retry(s.sbbCtx, func() error {
				platformBlockNumber, err = s.fetchPlatformBlockNumber(s.sbbCtx)
				return err
			}, 300*time.Millisecond)

			requestPlatformBlockNumber := *platformBlockNumber - 6
			finalizingBlockNumber := finalizedBlockNumber + 1

			validSequencerAddresses, sequencerRpcUrls, leaderSequencerIndex, err = s.fetchSequencerInfo(s.sbbCtx, requestPlatformBlockNumber, finalizingBlockNumber)
			if err != nil {
				log.Errorf("failed to fetch sequencer info, error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			log.Debug("Successfully updated sequencer info")

			err = s.finalizeBlock(s.sbbCtx, requestPlatformBlockNumber, finalizingBlockNumber, sequencerRpcUrls, leaderSequencerIndex, validSequencerAddresses)
			if err != nil {
				log.Errorf("failed to finalize block, error: %v", err)
				time.Sleep(100 * time.Millisecond)
				continue
			}

			time.Sleep(300 * time.Millisecond)

			Retry(s.sbbCtx, func() error {
				transactions, err := s.getRawTransactions(s.sbbCtx, finalizingBlockNumber, sequencerRpcUrls, leaderSequencerIndex)
				if err != nil {
					return err
				}
				s.blockTransactionsCh <- &BlockTransactions{blockNumber: finalizingBlockNumber, transactions: transactions}
				return nil
			}, 100*time.Millisecond)

			finalizedBlockNumber = finalizingBlockNumber

			endTime := time.Now().UnixMilli()
			duration := endTime - startTime
			var nextActionDelay time.Duration
			if loopTime-duration > 0 {
				nextActionDelay = time.Duration(loopTime - duration)
			} else {
				nextActionDelay = time.Duration(0)
			}

			time.Sleep(nextActionDelay * time.Millisecond)
		case <-s.sbbCtx.Done():
			return
		}
	}
}

func (s *SbbService) fetchPlatformBlockNumber(ctx context.Context) (*uint64, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second) // TODO: configuration
	defer reqCancel()

	platformBlockNumber, err := s.ethClient.BlockNumber(reqCtx)
	if err != nil {
		log.Error("failed to fetch platform block number", "error", err.Error())
		return nil, err
	}

	return &platformBlockNumber, nil
}

func (s *SbbService) fetchSequencerInfo(ctx context.Context, platformBlockNumber uint64, finalizeBlockNumber uint64) ([]string, []string, *uint64, error) {
	sequencerAddresses, err := s.fetchSequencerAddresses(ctx, platformBlockNumber)
	if err != nil {
		log.Error("failed to fetch sequencer addresses ", "error ", err.Error())
		return nil, nil, nil, err
	}

	validSequencerAddresses, sequencerRpcUrls, err := s.fetchSequencerRpcUrls(ctx, sequencerAddresses)
	if err != nil {
		log.Error("failed to fetch sequencer rpc urls ", "error ", err.Error())
		return nil, nil, nil, err
	}

	log.Debug("Successfully fetched sequencer info", " sequencerAddresses: ", sequencerAddresses, " sequencerRpcUrls: ", sequencerRpcUrls)

	leaderSequencerIndex, err := s.getLeaderSequencerIndex(finalizeBlockNumber, sequencerRpcUrls)
	if err != nil {
		log.Error("failed to get leader sequencer index ", "error ", err.Error())
		return nil, nil, nil, err
	}

	log.Debug("Successfully fetched leader sequencer index", " leaderSequencerIndex: ", *leaderSequencerIndex)

	return validSequencerAddresses, sequencerRpcUrls, leaderSequencerIndex, nil
}

func (s *SbbService) getLeaderSequencerIndex(finalizeBlockNumber uint64, sequencerRpcUrls []string) (*uint64, error) {

	if len(sequencerRpcUrls) < 1 {
		return nil, errors.New("there are no URLs available, making modular arithmetic impossible")
	}

	for i := 0; i < len(sequencerRpcUrls); i++ {
		if sequencerRpcUrls[i] == "http://210.222.63.26:5000" {
			index := uint64(i)
			return &index, nil
		}
	}

	mod := finalizeBlockNumber % uint64(len(sequencerRpcUrls))
	return &mod, nil
}

func (s *SbbService) fetchSequencerAddresses(ctx context.Context, platformBlockNumber uint64) ([]string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	contractAbi, err := abi.JSON(strings.NewReader(abiString))
	if err != nil {
		return nil, err
	}

	METHOD := "getSequencers"
	contractAddress := common.HexToAddress(s.blockchainService.Config().LivenessContractAddress)

	data, err := contractAbi.Pack(METHOD, s.blockchainService.Config().ClusterId)
	if err != nil {
		return nil, err
	}

	query := ethereum.CallMsg{
		To:   &contractAddress,
		Data: data,
	}

	result, err := s.ethClient.CallContract(reqCtx, query, big.NewInt(int64(platformBlockNumber)))
	if err != nil {
		log.Error("failed to make a contract call to retrieve the sequencer URL list", "error", err.Error())

		return nil, err
	}

	var sequencerList []common.Address
	if contractAbi.UnpackIntoInterface(&sequencerList, METHOD, result) != nil {
		return nil, err
	}

	var sequencerAddresses []string
	for _, addr := range sequencerList {
		if addr != common.HexToAddress("0x0000000000000000000000000000000000000000") {
			sequencerAddresses = append(sequencerAddresses, addr.Hex())
		}
	}

	return sequencerAddresses, nil
}

func (s *SbbService) fetchSequencerRpcUrls(ctx context.Context, sequencerAddresses []string) ([]string, []string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	body := newJsonRpcRequest(GetSequencerRpcUrlList, GetSequencerRpcUrlsParams{
		SequencerAddresses: sequencerAddresses,
	})

	res := &GetSequencerRpcUrlsResponse{}
	if err := s.sbbClient.Send(reqCtx, s.blockchainService.Config().SeedNodeUrl, body, res); err != nil {
		log.Error("failed to send get_sequencer_rpc_url_list request to seeder node", "error", err.Error())

		return nil, nil, err
	}

	var validSequencerAddresses []string
	var sequencerRpcUrls []string
	for _, sequencerRpcUrl := range res.SequencerRpcUrls {
		if sequencerRpcUrl.ClusterRpcUrl != "" {
			validSequencerAddresses = append(validSequencerAddresses, sequencerRpcUrl.Address)
			sequencerRpcUrls = append(sequencerRpcUrls, sequencerRpcUrl.ClusterRpcUrl)
		}
	}

	return validSequencerAddresses, sequencerRpcUrls, nil
}

func (s *SbbService) getNextLeaderSequencerIndex(sequencerCount uint64, currentLeaderSeqeuncerIndex uint64) (*uint64, error) {
	if sequencerCount < 1 {
		return nil, errors.New("cannot divide by zero")
	}

	nextLeaderSequencerIndex := (currentLeaderSeqeuncerIndex + 1) % sequencerCount

	return &nextLeaderSequencerIndex, nil
}

func (s *SbbService) increaseLeaderSequencerIndex(sequencerCount uint64, leaderSequencerIndex *uint64) error {
	if sequencerCount < 1 {
		return errors.New("cannot divide by zero")
	}

	*leaderSequencerIndex = (*leaderSequencerIndex + 1) % sequencerCount

	return nil
}

func (s *SbbService) finalizeBlock(ctx context.Context, platformBlockNumber uint64, finalizeBlockNumber uint64, sequencerRpcUrls []string, leaderSequencerIndex *uint64, sequencerAddresses []string) error {

	sequencerCount := len(sequencerRpcUrls)

	for i := 0; i < sequencerCount; i++ {
		nextSequencerIndex, err := s.getNextLeaderSequencerIndex(uint64(sequencerCount), *leaderSequencerIndex)
		if err != nil {
			return err
		}

		message := FinalizeBlockMessageParams{
			RollupId:                s.blockchainService.Config().RollupId,
			PlatformBlockHeight:     platformBlockNumber,
			RollupBlockHeight:       finalizeBlockNumber,
			BlockCreatorAddress:     strings.ToLower(sequencerAddresses[*leaderSequencerIndex]),
			NextBlockCreatorAddress: strings.ToLower(sequencerAddresses[*nextSequencerIndex]),
		}

		messageBytes, err := json.Marshal(message)
		if err != nil {
			log.Error("Error converting message to bytes: %v", err)
			return err
		}

		h := keccak256.Hash(messageBytes)

		signature, err := crypto.Sign(h, s.sequencerPrivateKey)
		if err != nil {
			log.Error("Error signing message: %v", err)
			return err
		}

		params := FinalizeBlockParams{
			Message:   message,
			Signature: "0x" + Bytes2Hex(signature),
		}

		log.Debug("Finalizing the contents to be included in the block", "block number: ", finalizeBlockNumber)

		body := newJsonRpcRequest(FinalizeBlock, params)

		reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
		defer reqCancel()

		if err = s.sbbClient.Send(reqCtx, sequencerRpcUrls[*leaderSequencerIndex], body, nil); err != nil {
			if !strings.Contains(err.Error(), "connection refused") {
				return fmt.Errorf("failed to send finalize_block request to SBB: %s request params: platformHeight %d rollupHeight %d url %s now %d", err.Error(), message.PlatformBlockHeight, message.RollupBlockHeight, sequencerRpcUrls[*leaderSequencerIndex], time.Now().UnixMilli())
			}

			log.Warn("failed to finalizing due to no sequencer found. retrying with a different sequencer")

			if err = s.increaseLeaderSequencerIndex(uint64(sequencerCount), leaderSequencerIndex); err != nil {
				return err
			}

			log.Debug("stopesi - Error", err)

			continue
		}

		log.Debug("Successfully finalized the contents to be included in the block. ", "block number: ", finalizeBlockNumber, " now: ", time.Now().UnixMilli())

		return nil
	}

	return errors.New("no sequencer")
}

func (s *SbbService) getRawTransactions(ctx context.Context, finalizedBlockNumber uint64, sequencerRpcUrls []string, leaderSequencerIndex *uint64) ([][]byte, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	params := GetRawTransactionsParams{
		RollupId:          s.blockchainService.Config().RollupId,
		RollupBlockHeight: finalizedBlockNumber,
	}

	body := newJsonRpcRequest(GetRawTransactionList, params)

	res := &GetRawTransactionsResponse{}
	sequencerCount := len(sequencerRpcUrls)

	for i := 0; i < sequencerCount; i++ {
		if err := s.sbbClient.Send(reqCtx, sequencerRpcUrls[*leaderSequencerIndex], body, res); err != nil {
			if !strings.Contains(err.Error(), "connection refused") {
				return nil, fmt.Errorf("failed to send get_raw_transaction_list request to SBB: %s height %d url %s now %d", err.Error(), params.RollupBlockHeight, sequencerRpcUrls[*leaderSequencerIndex], time.Now().UnixMilli())
			}

			log.Warn("failed to get raw transactions due to no sequencer found. retrying with a different sequencer")

			if err = s.increaseLeaderSequencerIndex(uint64(sequencerCount), leaderSequencerIndex); err != nil {
				return nil, err
			}
			continue
		}

		var encodedTxs [][]byte

		for _, hexStr := range res.RawTransactions {
			binary, err := hex.DecodeString(hexStr)
			if err != nil {
				return nil, fmt.Errorf("failed to decode transaction: %w", err)
			}
			encodedTxs = append(encodedTxs, binary)
		}
		return encodedTxs, nil
	}
	return nil, errors.New("no sequencer")
}

func Retry(ctx context.Context, fn func() error, retryInterval time.Duration) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := fn()
		if err == nil {
			return
		}
		time.Sleep(retryInterval)
	}
}

// finalizeBatches runs the endless loop for processing transactions finalizing batches.
func (s *SbbService) finalizeBatchesWithSbb(ctx context.Context) error {
	log.Debug("finalizer init loop with SBB")
	return nil
}

func Bytes2Hex(d []byte) string {
	return hex.EncodeToString(d)
}

func NewKeyFromKeystore(hexKey string) (*ecdsa.PrivateKey, error) {
	key, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, err
	}
	return key, nil
}

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

type GetSequencerRpcUrlsParams struct {
	SequencerAddresses []string `json:"sequencer_address_list"`
}

type SequencerRpcUrl struct {
	Address        string `json:"address"`
	ExternalRpcUrl string `json:"external_rpc_url"`
	ClusterRpcUrl  string `json:"cluster_rpc_url"`
}

type GetSequencerRpcUrlsResponse struct {
	SequencerRpcUrls []SequencerRpcUrl `json:"sequencer_rpc_url_list"`
}

type FinalizeBlockMessageParams struct {
	RollupId        string         `json:"rollup_id"`
	ExecutorAddress common.Address `json:"executor_address"`

	PlatformBlockHeight uint64 `json:"platform_block_height"`
	RollupBlockHeight   uint64 `json:"rollup_block_height"`

	BlockCreatorAddress     string `json:"block_creator_address"`
	NextBlockCreatorAddress string `json:"next_block_creator_address"`
}

type FinalizeBlockParams struct {
	Message   FinalizeBlockMessageParams `json:"finalize_block_message"`
	Signature string                     `json:"signature"`
}

type GetRawTransactionsParams struct {
	RollupId          string `json:"rollup_id"`
	RollupBlockHeight uint64 `json:"rollup_block_height"`
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
