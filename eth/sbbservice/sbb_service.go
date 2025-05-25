package sbbservice

import (
	"context"
	"encoding/hex"
	"fmt"
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
	lighthouseWsClient       *lighthousewsclient.LighthouseWsClient
	blockchain               BlockchainService
	httpClient               *httpclient.HttpClient
	auctionCreatedSlotNumber int64
	fetchedTxsSlotNumber     int64
	slotTransactionsCh       chan *SlotTransactions
	filter                   *rpchelper.Filters
}

func NewSbbService(config *ethconfig.Config, blockchainService BlockchainService, LighthouseWsClient *lighthousewsclient.LighthouseWsClient) (*SbbService, error) {
	httpClient := httpclient.New()

	return &SbbService{
		Config:                   config,
		lighthouseWsClient:       LighthouseWsClient,
		blockchain:               blockchainService,
		httpClient:               httpClient,
		auctionCreatedSlotNumber: -1,
		fetchedTxsSlotNumber:     -1,
		slotTransactionsCh:       make(chan *SlotTransactions, config.MaxSbbFinalizationCapacity),
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

			var auctionStartTimestamp *uint64 = nil
			var err error
			if s.lighthouseWsClient != nil && s.auctionCreatedSlotNumber <= s.fetchedTxsSlotNumber+1 {
				creatingAuctionSlotNumber := s.fetchedTxsSlotNumber + 2
				auctionStartTimestamp, err = s.lighthouseWsClient.CreateAuction(creatingAuctionSlotNumber, s.SlotTime)
				if err != nil {
					fmt.Println("failed to create auction, error: ", err.Error())
				} else {
					s.auctionCreatedSlotNumber = creatingAuctionSlotNumber
				}
			}

			if err = Retry(ctx, func() error {
				transactions, rawTransactions, err := s.getRawTransactions(ctx, auctionStartTimestamp)
				if err != nil {
					fmt.Println("failed to get raw transactions, error: ", err.Error())
					return err
				}
				s.slotTransactionsCh <- &SlotTransactions{transactions: transactions}
				s.filter.OnNewBobTxs(rawTransactions)
				return nil
			}, 100*time.Millisecond); err != nil {
				fmt.Printf("getRawTransactions error: %v", err)
				timer.Reset(100 * time.Millisecond)
				break
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

func (s *SbbService) getRawTransactions(ctx context.Context, auctionStartTimestamp *uint64) ([][]byte, []string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	fetchingTxsSlotNumber := s.fetchedTxsSlotNumber + 1

	params := GetRawTransactionsParams{
		RollupId:                      s.RollupId,
		Mode:                          s.Mode,
		SlotNumber:                    fetchingTxsSlotNumber,
		NextSlotAuctionCreated:        s.auctionCreatedSlotNumber == fetchingTxsSlotNumber+1,
		NextSlotAuctionStartTimestamp: auctionStartTimestamp,
	}

	body := newJsonRpcRequest(GetRawTransactionList, params)

	res := &GetRawTransactionsResponse{}
	if err := s.httpClient.Send(reqCtx, s.SbbUrl, body, res); err != nil {
		return nil, nil, err
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

	s.fetchedTxsSlotNumber = fetchingTxsSlotNumber

	logger.ColorPrintln(logger.Green, "Transaction processing succeeded. tx count: "+strconv.Itoa(len(res.RawTransactions))+" slot number: "+strconv.FormatInt(s.fetchedTxsSlotNumber, 10))

	return encodedTxs, res.RawTransactions, nil
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
