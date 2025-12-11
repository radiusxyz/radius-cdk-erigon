package sbbclient

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
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethclient"
	"github.com/ledgerwatch/erigon/httpclient"
	"github.com/ledgerwatch/erigon/zkevm/log"
	"math/big"
	"sort"
	"strings"
	"time"
)

type Method string

const (
	FinalizeBlock          Method = "finalize_block"
	GetRawTransactionList  Method = "get_raw_transaction_list"
	GetTxOrdererRpcUrlList Method = "get_tx_orderer_rpc_info_list"
)

type BlockchainService interface {
	SubmitRawTransactions(ctx context.Context, encodedTx [][]byte) error
	GetBlockNumber() (*uint64, error)
	BlockCreationCh() chan struct{}
	Config() *ethconfig.Config
}

type BlockTransactions struct {
	transactions [][]byte
}

type SbbClient struct {
	httpClient          *httpclient.HttpClient
	ethClient           *ethclient.Client
	blockchainService   BlockchainService
	sequencerPrivateKey *ecdsa.PrivateKey
	blockTransactionsCh chan *BlockTransactions
	sbbCtx              context.Context
}

func NewSbbClient(ctx context.Context, blockchainService BlockchainService) (*SbbClient, error) {
	httpClient := httpclient.New()
	ethClient, _ := ethclient.Dial(blockchainService.Config().PlatformUrl) // TODO: error handling
	sequencerPrivateKey, err := NewPrivateKeyFromHexKey(blockchainService.Config().SequencerPrivateKey)
	if err != nil {
		log.Fatal(err)
	}

	return &SbbClient{
		httpClient:          httpClient,
		ethClient:           ethClient,
		blockchainService:   blockchainService,
		sequencerPrivateKey: sequencerPrivateKey,
		blockTransactionsCh: make(chan *BlockTransactions, blockchainService.Config().MaxSbbFinalizationCapacity),
		sbbCtx:              ctx,
	}, nil
}

func (s *SbbClient) Start() {
	log.Info("Starting sbb service...")
	go s.requestToSbb()
	go s.insertTransactions()
}

func (s *SbbClient) insertTransactions() {
	for {
		select {
		case blockTransactions := <-s.blockTransactionsCh:
			if len(blockTransactions.transactions) > 0 {
				if err := s.blockchainService.SubmitRawTransactions(s.sbbCtx, blockTransactions.transactions); err != nil {
					panic("SubmitRawTransactions" + err.Error())
				}
			}
		case <-s.sbbCtx.Done():
			return
		}
	}
}

func (s *SbbClient) requestToSbb() {
	loopTime := int64(2000)
	timer := time.NewTimer(time.Duration(loopTime) * time.Millisecond)

	var platformBlockNumber *uint64
	var validTxOrdererAddresses []string
	var txOrdererRpcUrls []string
	var leaderTxOrdererIndex *uint64

	for {
		select {
		case <-timer.C:
			startTime := time.Now().UnixMilli()

			var err error
			if err = RetryWithCount(s.sbbCtx, func() error {
				platformBlockNumber, err = s.fetchPlatformBlockNumber(s.sbbCtx)
				return err
			}, 300*time.Millisecond, 10); err != nil {
				log.Errorf("fetchPlatformBlockNumber error: %v", err)
				timer.Reset(100 * time.Millisecond)
				break
			}

			requestPlatformBlockNumber := *platformBlockNumber - 6
			blockNumber, err := s.blockchainService.GetBlockNumber()
			if err != nil {
				timer.Reset(100 * time.Millisecond)
				break
			}

			validTxOrdererAddresses, txOrdererRpcUrls, leaderTxOrdererIndex, err = s.fetchTxOrdererInfo(s.sbbCtx, requestPlatformBlockNumber, *blockNumber+1)
			if err != nil {
				log.Errorf("failed to fetch tx_orderer info, error: %v", err)
				timer.Reset(100 * time.Millisecond)
				break
			}

			log.Debug("Successfully updated tx_orderer info")

			transactions, err := s.getRawTransactions(s.sbbCtx, txOrdererRpcUrls, leaderTxOrdererIndex, validTxOrdererAddresses, requestPlatformBlockNumber)
			if err != nil {
				log.Errorf("failed to get raw transactions, error: %v", err)
				timer.Reset(100 * time.Millisecond)
				break
			}

			s.blockTransactionsCh <- &BlockTransactions{transactions: transactions}

			endTime := time.Now().UnixMilli()
			duration := endTime - startTime
			if loopTime-duration > 0 {
				timer.Reset(time.Duration(loopTime-duration) * time.Millisecond)
			} else {
				timer.Reset(0)
			}
		case <-s.sbbCtx.Done():
			return
		}
	}
}

func (s *SbbClient) fetchPlatformBlockNumber(ctx context.Context) (*uint64, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second) // TODO: configuration
	defer reqCancel()

	platformBlockNumber, err := s.ethClient.BlockNumber(reqCtx)
	if err != nil {
		log.Error("failed to fetch platform block number", "error", err.Error())
		return nil, err
	}
	return &platformBlockNumber, nil
}

func (s *SbbClient) fetchTxOrdererInfo(ctx context.Context, platformBlockNumber uint64, blockNumber uint64) ([]string, []string, *uint64, error) {
	txOrdererAddresses, err := s.fetchTxOrdererAddresses(ctx, platformBlockNumber)
	if err != nil {
		log.Error("failed to fetch tx_orderer addresses ", "error ", err.Error())
		return nil, nil, nil, err
	}

	validTxOrdererAddresses, txOrdererRpcUrls, err := s.fetchTxOrdererRpcUrls(ctx, txOrdererAddresses)
	if err != nil {
		log.Error("failed to fetch tx_orderer rpc urls ", "error ", err.Error())
		return nil, nil, nil, err
	}

	log.Debug("Successfully fetched tx_orderer info", " txOrdererAddresses: ", txOrdererAddresses, " txOrdererRpcUrls: ", txOrdererRpcUrls)

	leaderTxOrdererIndex, err := s.getLeaderTxOrdererIndex(blockNumber, txOrdererRpcUrls)
	if err != nil {
		log.Error("failed to get leader tx_orderer index ", "error ", err.Error())
		return nil, nil, nil, err
	}

	log.Debug("Successfully fetched leader tx_orderer index", " leaderTxOrdererIndex: ", *leaderTxOrdererIndex)

	return validTxOrdererAddresses, txOrdererRpcUrls, leaderTxOrdererIndex, nil
}

func (s *SbbClient) getLeaderTxOrdererIndex(finalizeBlockNumber uint64, txOrdererRpcUrls []string) (*uint64, error) {

	if len(txOrdererRpcUrls) < 1 {
		return nil, errors.New("there are no URLs available, making modular arithmetic impossible")
	}

	for i := 0; i < len(txOrdererRpcUrls); i++ {
		if txOrdererRpcUrls[i] == "http://210.222.63.26:5000" {
			index := uint64(i)
			return &index, nil
		}
	}

	mod := finalizeBlockNumber % uint64(len(txOrdererRpcUrls))
	return &mod, nil
}

func (s *SbbClient) fetchTxOrdererAddresses(ctx context.Context, platformBlockNumber uint64) ([]string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
	defer reqCancel()

	contractAbi, err := abi.JSON(strings.NewReader(abiString))
	if err != nil {
		return nil, err
	}

	METHOD := "getTxOrderers"
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
		log.Error("failed to make a contract call to retrieve the tx_orderer URL list", "error", err.Error())

		return nil, err
	}

	var txOrdererList []common.Address
	if contractAbi.UnpackIntoInterface(&txOrdererList, METHOD, result) != nil {
		return nil, err
	}

	var txOrdererAddresses []string
	for _, addr := range txOrdererList {
		if addr != common.HexToAddress("0x0000000000000000000000000000000000000000") {
			txOrdererAddresses = append(txOrdererAddresses, addr.Hex())
		}
	}

	return txOrdererAddresses, nil
}

func (s *SbbClient) fetchTxOrdererRpcUrls(ctx context.Context, txOrdererAddresses []string) ([]string, []string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
	defer reqCancel()

	body := newJsonRpcRequest(GetTxOrdererRpcUrlList, GetTxOrdererRpcUrlsParams{
		TxOrdererAddresses: txOrdererAddresses,
	})

	res := &GetTxOrdererRpcUrlsResponse{}
	if err := s.httpClient.Send(reqCtx, s.blockchainService.Config().SeedNodeUrl, body, res); err != nil {
		log.Error("failed to send get_tx_orderer_rpc_url_list request to seeder node", "error", err.Error())

		return nil, nil, err
	}

	var validTxOrdererAddresses []string
	var txOrdererRpcUrls []string
	for _, txOrdererRpcUrl := range res.TxOrdererRpcUrls {
		if txOrdererRpcUrl.ClusterRpcUrl != "" {
			validTxOrdererAddresses = append(validTxOrdererAddresses, txOrdererRpcUrl.Address)
			txOrdererRpcUrls = append(txOrdererRpcUrls, txOrdererRpcUrl.ClusterRpcUrl)
		}
	}

	return validTxOrdererAddresses, txOrdererRpcUrls, nil
}

func (s *SbbClient) getNextLeaderTxOrdererIndex(txOrdererCount uint64, currentLeaderSeqeuncerIndex uint64) (*uint64, error) {
	if txOrdererCount < 1 {
		return nil, errors.New("cannot divide by zero")
	}

	nextLeaderTxOrdererIndex := (currentLeaderSeqeuncerIndex + 1) % txOrdererCount

	return &nextLeaderTxOrdererIndex, nil
}

func (s *SbbClient) increaseLeaderTxOrdererIndex(txOrdererCount uint64, leaderTxOrdererIndex *uint64) error {
	if txOrdererCount < 1 {
		return errors.New("cannot divide by zero")
	}

	*leaderTxOrdererIndex = (*leaderTxOrdererIndex + 1) % txOrdererCount

	return nil
}

func (s *SbbClient) getRawTransactions(ctx context.Context, txOrdererRpcUrls []string, leaderTxOrdererIndex *uint64, txOrdererAddresses []string, platformBlockHeight uint64) ([][]byte, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
	defer reqCancel()

	res := &GetRawTransactionsResponse{}
	txOrdererCount := len(txOrdererRpcUrls)

	for i := 0; i < txOrdererCount; i++ {
		nextTxOrdererIndex, err := s.getNextLeaderTxOrdererIndex(uint64(txOrdererCount), *leaderTxOrdererIndex)
		if err != nil {
			return nil, err
		}

		leaderChangeMessage := LeaderChangeMessage{
			RollupId:                      s.blockchainService.Config().RollupId,
			ExecutorAddress:               "0xE34aaF64b29273B7D567FCFc40544c014EEe9970",
			CurrentLeaderTxOrdererAddress: strings.ToLower(txOrdererAddresses[*leaderTxOrdererIndex]),
			NextLeaderTxOrdererAddress:    strings.ToLower(txOrdererAddresses[*nextTxOrdererIndex]),
			PlatformBlockHeight:           platformBlockHeight,
		}

		messageBytes, err := json.Marshal(leaderChangeMessage)
		if err != nil {
			log.Error("Error converting message to bytes: %v", err)
			return nil, err
		}

		h := keccak256.Hash(messageBytes)

		signature, err := crypto.Sign(h, s.sequencerPrivateKey)
		if err != nil {
			log.Error("Error signing message: %v", err)
			return nil, err
		}

		params := GetRawTransactionsParams{
			LeaderChangeMessage: leaderChangeMessage,
			RollupSignature:     "0x" + Bytes2Hex(signature),
		}

		body := newJsonRpcRequest(GetRawTransactionList, params)

		if err := s.httpClient.Send(reqCtx, txOrdererRpcUrls[*leaderTxOrdererIndex], body, res); err != nil {
			if !strings.Contains(err.Error(), "connection refused") {
				return nil, fmt.Errorf("failed to send get_raw_transaction_list request to SBB: %s url %s now %d", err.Error(), txOrdererRpcUrls[*leaderTxOrdererIndex], time.Now().UnixMilli())
			}

			log.Warn("failed to get raw transactions due to no tx_orderer found. retrying with a different tx_orderer")

			if err = s.increaseLeaderTxOrdererIndex(uint64(txOrdererCount), leaderTxOrdererIndex); err != nil {
				return nil, err
			}
			continue
		}

		// --- Transaction Sorting Logic Start ---

		type txInfo struct {
			sender   common.Address
			nonce    uint64
			rawBytes []byte
		}

		parsedTxs := make([]*txInfo, len(res.RawTransactions))
		slotsBySender := make(map[common.Address][]int)

		for idx, hexStr := range res.RawTransactions {
			log.Info("Original Ordering", "index: ", idx, " hexStr: ", hexStr)
			// 1. Hex Decode
			hexStr = strings.TrimPrefix(hexStr, "0x")
			txBytes, err := hex.DecodeString(hexStr)
			if err != nil {
				return nil, fmt.Errorf("failed to decode transaction hex at index %d: %w", idx, err)
			}

			// 2. RLP Decode
			tx, err := types.DecodeTransaction(txBytes)
			if err != nil {
				return nil, fmt.Errorf("failed to rlp decode transaction at index %d: %w", idx, err)
			}

			// 3. Sender Recovery (Config 의존성 제거)
			//sender,  := tx.GetSender()

			chainID := big.NewInt(10101) // 반드시 실제 체인 ID 입력!
			signer := types.LatestSignerForChainID(chainID)

			sender, err := signer.Sender(tx) // <= 여기!
			if err != nil {
				log.Error("failed to recover sender from tx",
					"index", idx,
					"err", err,
					"nonce", tx.GetNonce(),
				)
				//return nil, fmt.Errorf("failed to recover sender at index %d: %w", idx, err)
			}

			// 4. 정보 저장
			parsedTxs[idx] = &txInfo{
				sender:   sender,
				nonce:    tx.GetNonce(),
				rawBytes: txBytes,
			}
			slotsBySender[sender] = append(slotsBySender[sender], idx)
		}

		// 5. Sender별 Nonce 정렬 및 재배치
		for _, indices := range slotsBySender {
			senderTxs := make([]*txInfo, len(indices))
			for k, originalIdx := range indices {
				senderTxs[k] = parsedTxs[originalIdx]
			}

			sort.Slice(senderTxs, func(i, j int) bool {
				return senderTxs[i].nonce < senderTxs[j].nonce
			})

			for k, originalIdx := range indices {
				parsedTxs[originalIdx] = senderTxs[k]
			}
		}

		// 6. 최종 결과 생성 및 로그
		var encodedTxs [][]byte
		var logMsgBuilder strings.Builder
		logMsgBuilder.WriteString("Sorted Transactions Order: ")

		for i, info := range parsedTxs {
			encodedTxs = append(encodedTxs, info.rawBytes)

			shortSender := info.sender.Hex()
			if len(shortSender) > 8 {
				shortSender = shortSender[:8] + ".."
			}
			logMsgBuilder.WriteString(fmt.Sprintf("[%d] %s: %d | ", i, shortSender, info.nonce))
		}

		log.Info(logMsgBuilder.String(), "total_count", len(encodedTxs), "now", time.Now().UnixMilli())

		log.Info("Transaction processing succeeded.", "tx count: ", len(encodedTxs), " now: ", time.Now().UnixMilli())

		return encodedTxs, nil
	}
	return nil, errors.New("no tx_orderer")
}

//func (s *SbbClient) getRawTransactions(ctx context.Context, txOrdererRpcUrls []string, leaderTxOrdererIndex *uint64, txOrdererAddresses []string, platformBlockHeight uint64) ([][]byte, error) {
//	reqCtx, reqCancel := context.WithTimeout(ctx, 10*time.Second)
//	defer reqCancel()
//
//	res := &GetRawTransactionsResponse{}
//	txOrdererCount := len(txOrdererRpcUrls)
//
//	for i := 0; i < txOrdererCount; i++ {
//		nextTxOrdererIndex, err := s.getNextLeaderTxOrdererIndex(uint64(txOrdererCount), *leaderTxOrdererIndex)
//		if err != nil {
//			return nil, err
//		}
//
//		leaderChangeMessage := LeaderChangeMessage{
//			RollupId:                      s.blockchainService.Config().RollupId,
//			ExecutorAddress:               "0xE34aaF64b29273B7D567FCFc40544c014EEe9970",
//			CurrentLeaderTxOrdererAddress: strings.ToLower(txOrdererAddresses[*leaderTxOrdererIndex]),
//			NextLeaderTxOrdererAddress:    strings.ToLower(txOrdererAddresses[*nextTxOrdererIndex]),
//			PlatformBlockHeight:           platformBlockHeight,
//		}
//
//		messageBytes, err := json.Marshal(leaderChangeMessage)
//		if err != nil {
//			log.Error("Error converting message to bytes: %v", err)
//			return nil, err
//		}
//
//		h := keccak256.Hash(messageBytes)
//
//		signature, err := crypto.Sign(h, s.sequencerPrivateKey)
//		if err != nil {
//			log.Error("Error signing message: %v", err)
//			return nil, err
//		}
//
//		params := GetRawTransactionsParams{
//			LeaderChangeMessage: leaderChangeMessage,
//			RollupSignature:     "0x" + Bytes2Hex(signature),
//		}
//
//		body := newJsonRpcRequest(GetRawTransactionList, params)
//
//		if err := s.httpClient.Send(reqCtx, txOrdererRpcUrls[*leaderTxOrdererIndex], body, res); err != nil {
//			if !strings.Contains(err.Error(), "connection refused") {
//				return nil, fmt.Errorf("failed to send get_raw_transaction_list request to SBB: %s url %s now %d", err.Error(), txOrdererRpcUrls[*leaderTxOrdererIndex], time.Now().UnixMilli())
//			}
//
//			log.Warn("failed to get raw transactions due to no tx_orderer found. retrying with a different tx_orderer")
//
//			if err = s.increaseLeaderTxOrdererIndex(uint64(txOrdererCount), leaderTxOrdererIndex); err != nil {
//				return nil, err
//			}
//			continue
//		}
//
//		//here
//
//		var encodedTxs [][]byte
//
//		for _, hexStr := range res.RawTransactions {
//			hexStr = strings.TrimPrefix(hexStr, "0x")
//			binary, err := hex.DecodeString(hexStr)
//			if err != nil {
//				return nil, fmt.Errorf("failed to decode transaction: %w", err)
//			}
//			encodedTxs = append(encodedTxs, binary)
//		}
//
//		log.Info("Transaction processing succeeded.", "tx count: ", len(encodedTxs), " now: ", time.Now().UnixMilli())
//
//		return encodedTxs, nil
//	}
//	return nil, errors.New("no tx_orderer")
//}

func RetryWithCount(ctx context.Context, fn func() error, retryInterval time.Duration, retryCount int) error {
	for i := 0; i < retryCount; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
	return errors.New("the retry limit has been exceeded")
}

func Retry(ctx context.Context, fn func() error, retryInterval time.Duration) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		err := fn()
		if err == nil {
			return nil
		}
		time.Sleep(retryInterval)
	}
}

func Bytes2Hex(d []byte) string {
	return hex.EncodeToString(d)
}

func NewPrivateKeyFromHexKey(hexKey string) (*ecdsa.PrivateKey, error) {
	hexKey = strings.TrimPrefix(hexKey, "0x")
	key, err := crypto.HexToECDSA(hexKey)
	if err != nil {
		return nil, err
	}
	return key, nil
}
