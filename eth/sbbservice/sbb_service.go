package sbbservice

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/eth/lighthouseservice"
	"github.com/ledgerwatch/erigon/httpclient"
	"github.com/ledgerwatch/erigon/logger"
	"strconv"
	"time"
)

type Method string

const (
	FinalizeSlot          Method = "rollup_finalizeSlot"
	GetRawTransactionList Method = "rollup_getRawTransactions"
)

type SlotTransactions struct {
	slotNumber   int64
	transactions []string
}

type SbbService struct {
	mode                     string
	LighthouseService        *lighthouseservice.LighthouseService
	blockchain               *blockchain.Blockchain
	httpClient               *httpclient.HttpClient
	rollupId                 string
	auctionCreatedSlotNumber int64
	fetchedTxsSlotNumber     int64
	slotTransactionsCh       chan *SlotTransactions
	sbbUrl                   string
}

func NewSbbService(LighthouseService *lighthouseservice.LighthouseService, mode string, rollupId string, sbbUrl string) (*SbbService, error) {
	httpClient := httpclient.New()

	bc := blockchain.NewBlockchain(dataDir)

	return &SbbService{
		mode:                     mode,
		LighthouseService:        LighthouseService,
		blockchain:               bc,
		httpClient:               httpClient,
		rollupId:                 rollupId,
		auctionCreatedSlotNumber: -1,
		fetchedTxsSlotNumber:     -1,
		slotTransactionsCh:       make(chan *SlotTransactions, conf.MaxSbbFinalizationCapacity),
		sbbUrl:                   sbbUrl,
	}, nil
}

func (s *SbbService) Start(ctx context.Context) {
	log.Info("Starting sbb service...")
	go s.requestToSbb(ctx)
	go s.insertTransactions(ctx)
}

func (s *SbbService) insertTransactions(ctx context.Context) {
	for {
		select {
		case slotTransactions := <-s.slotTransactionsCh:
			if err := s.blockchain.SubmitRawTransactions(slotTransactions.slotNumber, slotTransactions.transactions); err != nil {
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

			if s.LighthouseService != nil && s.auctionCreatedSlotNumber <= s.fetchedTxsSlotNumber+1 {
				creatingAuctionSlotNumber := s.fetchedTxsSlotNumber + 2
				if err := s.LighthouseService.CreateAuction(creatingAuctionSlotNumber, s.SlotTime); err != nil {
					fmt.Println("failed to create auction, error: ", err.Error())
				} else {
					s.auctionCreatedSlotNumber = creatingAuctionSlotNumber
				}
			}

			if err := Retry(ctx, func() error {
				transactions, err := s.getRawTransactions(ctx)
				if err != nil {
					fmt.Println("failed to get raw transactions, error: ", err.Error())
					return err
				}
				s.slotTransactionsCh <- &SlotTransactions{slotNumber: s.fetchedTxsSlotNumber, transactions: transactions}
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

func (s *SbbService) getRawTransactions(ctx context.Context) ([]string, error) {
	reqCtx, reqCancel := context.WithTimeout(ctx, 2*time.Second)
	defer reqCancel()

	fetchingTxsSlotNumber := s.fetchedTxsSlotNumber + 1

	params := GetRawTransactionsParams{
		RollupId:               s.rollupId,
		Mode:                   s.mode,
		SlotNumber:             fetchingTxsSlotNumber,
		NextSlotAuctionCreated: s.auctionCreatedSlotNumber == fetchingTxsSlotNumber+1,
	}

	body := newJsonRpcRequest(GetRawTransactionList, params)

	res := &GetRawTransactionsResponse{}
	if err := s.httpClient.Send(reqCtx, s.sbbUrl, body, res); err != nil {
		return nil, err
	}

	s.fetchedTxsSlotNumber = fetchingTxsSlotNumber

	logger.ColorPrintln(logger.Green, "Transaction processing succeeded. tx count: "+strconv.Itoa(len(res.RawTransactions))+" slot number: "+strconv.FormatInt(s.fetchedTxsSlotNumber, 10))

	return res.RawTransactions, nil
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
