package sbbservice

import (
	"context"
	"encoding/hex"
	"fmt"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/lighthousewsclient"
	"github.com/ledgerwatch/erigon/httpclient"
	"github.com/ledgerwatch/erigon/logger"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"strconv"
	"strings"
	"time"
)

type Method string

const (
	FinalizeSlot          Method = "rollup_finalizeSlot"
	GetRawTransactionList Method = "rollup_getRawTransactions"
)

type SlotTransactions struct {
	transactions [][]byte
}

type BlockchainService interface {
	SubmitRawTransactions(ctx context.Context, encodedTx [][]byte) error
	GetBlockNumber() (*uint64, error)
	Config() *ethconfig.Config
}

type SbbService struct {
	*ethconfig.Config
	lighthouseWsClient   *lighthousewsclient.LighthouseWsClient
	blockchain           BlockchainService
	httpClient           *httpclient.HttpClient
	fetchedTxsSlotNumber int64
	slotTransactionsCh   chan *SlotTransactions
	filter               *rpchelper.Filters
	lighthouseTxsCh      chan *common.LighthouseTransactions
}

func NewSbbService(config *ethconfig.Config, blockchainService BlockchainService, LighthouseWsClient *lighthousewsclient.LighthouseWsClient, lighthouseTxsCh chan *common.LighthouseTransactions) (*SbbService, error) {
	httpClient := httpclient.New()

	return &SbbService{
		Config:               config,
		lighthouseWsClient:   LighthouseWsClient,
		blockchain:           blockchainService,
		httpClient:           httpClient,
		fetchedTxsSlotNumber: -1,
		slotTransactionsCh:   make(chan *SlotTransactions, config.MaxSbbFinalizationCapacity),
		lighthouseTxsCh:      lighthouseTxsCh,
	}, nil
}

func (s *SbbService) Start(ctx context.Context) {
	logger.Println("Starting sbb service...")
	go s.requestToSbb(ctx)
	go s.insertTransactions(ctx)
}

func (s *SbbService) SetFilter(filter *rpchelper.Filters) {
	s.filter = filter
}

func (s *SbbService) insertTransactions(ctx context.Context) {
	for {
		select {
		case slotTransactions := <-s.slotTransactionsCh:
			if err := s.blockchain.SubmitRawTransactions(ctx, slotTransactions.transactions); err != nil {
				panic("youngmin - SubmitRawTransactions" + err.Error())
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *SbbService) requestToSbb(ctx context.Context) {
	loopTime := int64(s.SlotTime)
	timer := time.NewTimer(time.Duration(loopTime) * time.Millisecond)

	for {
		select {
		case <-timer.C:
			startTime := time.Now().UnixMilli()

			txs := make([][]byte, 0)

			if err := Retry(ctx, func() error {
				rawTransactions, err := s.getRawTransactions(ctx)
				if err != nil {
					fmt.Println("failed to get raw transactions, error: ", err.Error())
					return err
				}
				txs = append(txs, rawTransactions...)
				return nil
			}, 100*time.Millisecond); err != nil {
				fmt.Printf("getRawTransactions error: %v", err)
				timer.Reset(100 * time.Millisecond)
				break
			}

			if s.lighthouseWsClient != nil && s.fetchedTxsSlotNumber > -1 {
				timeout := time.After(5 * time.Second)
				select {
				case lighthouseTxs := <-s.lighthouseTxsCh:
					if lighthouseTxs.SlotNumber > s.fetchedTxsSlotNumber {
						panic("invalid LH slotNumber: lh: " + strconv.FormatInt(lighthouseTxs.SlotNumber, 10) + " curSlot: " + strconv.FormatInt(s.fetchedTxsSlotNumber, 10))
					} else if lighthouseTxs.SlotNumber < s.fetchedTxsSlotNumber {
						logger.ColorPrintf(logger.Yellow, "Warning: Discard LighthouseTx due to LH slotNumber(%d) is too low. current slotNumber(%d)", lighthouseTxs.SlotNumber, s.fetchedTxsSlotNumber+1)
					} else {
						txs = append(lighthouseTxs.RawTransactions, txs...)
					}

				case <-timeout:
					logger.ColorPrintf(logger.Yellow, "Warning: Timed out waiting for lighthouse transactions for slot %d", s.fetchedTxsSlotNumber+1)
				}
			}

			s.slotTransactionsCh <- &SlotTransactions{transactions: txs}
			s.filter.OnNewSlotTxs(&types.SlotTransactions{SlotNumber: s.fetchedTxsSlotNumber, RawTransactions: txs})

			if s.lighthouseWsClient != nil {
				err := s.lighthouseWsClient.CreateAuction(s.fetchedTxsSlotNumber+1, s.SlotTime)
				if err != nil {
					fmt.Println("failed to create auction, error: ", err.Error())
				}
			}

			endTime := time.Now().UnixMilli()
			duration := endTime - startTime
			if loopTime-duration > 0 {
				timer.Reset(time.Duration(loopTime-duration) * time.Millisecond)
			} else {
				timer.Reset(0)
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *SbbService) getRawTransactions(ctx context.Context) ([][]byte, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	params := GetRawTransactionsParams{
		RollupId:   s.RollupId,
		Mode:       s.Mode,
		SlotNumber: s.fetchedTxsSlotNumber + 1,
	}

	body := newJsonRpcRequest(GetRawTransactionList, params)

	res := &GetRawTransactionsResponse{}
	if err := s.httpClient.Send(reqCtx, s.SbbUrl, body, res); err != nil {
		return nil, err
	}

	var encodedTxs [][]byte

	for _, hexStr := range res.RawTransactions {
		hexStr = strings.TrimPrefix(hexStr, "0x")
		binary, err := hex.DecodeString(hexStr)
		if err != nil {
			logger.ColorPrintln(logger.Red, "failed to decode transaction: "+hexStr)
		}
		encodedTxs = append(encodedTxs, binary)
	}

	s.fetchedTxsSlotNumber += 1

	logger.ColorPrintln(logger.Green, "Transaction processing succeeded. tx count: "+strconv.Itoa(len(res.RawTransactions))+" slot number: "+strconv.FormatInt(s.fetchedTxsSlotNumber, 10))

	return encodedTxs, nil
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
