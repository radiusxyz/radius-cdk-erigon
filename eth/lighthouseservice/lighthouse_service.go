package lighthouseservice

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/lighthouseservice/requests"
	"github.com/ledgerwatch/erigon/logger"
	"io"
	"strconv"
	"strings"
)

type LighthouseService struct {
	conn             *websocket.Conn
	rollupId         string
	rollupPrivateKey string
	sbbUrl           string
	leaveCh          chan struct{}
	envelopeCh       chan []byte
	handler          *LighthouseMessageHandler
}

func NewLighthouseService(lighthouseUrl string, rollupId string, rollupPrivateKey string, sbbUrl string) (*LighthouseService, error) {
	conn, _, err := websocket.DefaultDialer.Dial(lighthouseUrl, nil)
	if err != nil {
		return nil, err
	}

	return &LighthouseService{
		conn:             conn,
		rollupId:         rollupId,
		rollupPrivateKey: rollupPrivateKey,
		sbbUrl:           sbbUrl,
		leaveCh:          make(chan struct{}),
		envelopeCh:       make(chan []byte),
		handler:          NewLighthouseMessageHandler(conn),
	}, nil
}

func (l *LighthouseService) Start(ctx context.Context) {
	for i := 0; i < 1; i++ {
		go l.ManageCh()
	}

	go l.ReadMessage()

	signature, err := GetSignature(l.rollupId, l.rollupPrivateKey)
	if err != nil {
		panic(err)
	}

	verifyRollupRequest := &requests.VerifyRollupRequest{
		RollupId:  l.rollupId,
		Signature: signature,
	}

	if err = l.handler.SendMessage(requests.VerifyRollup, verifyRollupRequest); err != nil {
		logger.Println("Write error:", err)
	}

	logger.Printf("rollup(%s) verification message sent", l.rollupId)
}

func (l *LighthouseService) CreateAuction(slotNumber int64, slotTime int) error {
	createAuctionRequest := &requests.CreateAuctionRequest{
		RollupId:           l.rollupId,
		SlotNumber:         slotNumber,
		SlotTime:           slotTime,
		LeaderTxOrdererUrl: l.sbbUrl,
	}
	requestType := requests.CreateAuction
	if err := l.handler.SendMessage(requestType, createAuctionRequest); err != nil {
		return err
	}

	logger.ColorPrintln(logger.BrightCyan, "Sent auction creation message. slotNumber: "+strconv.FormatInt(slotNumber, 10))

	return nil
}

func (l *LighthouseService) ReadMessage() {
	defer func() {
		l.leaveCh <- struct{}{}
	}()

	for {
		_, message, err := l.conn.ReadMessage()
		if err != nil {
			logger.Println("Read error:", err)
			if errors.Is(err, io.EOF) {
				fmt.Println("youngmin - eof")
				l.leaveCh <- struct{}{}
			}
			break
		}
		l.envelopeCh <- message
	}
}

func (l *LighthouseService) ManageCh() {
	for {
		select {
		case <-l.leaveCh:
			_ = l.conn.Close()
			logger.Println("Connection to the server has been lost")
		case envelope := <-l.envelopeCh:
			if err := l.handler.HandleEnvelope(envelope); err != nil {
				logger.ColorPrintf(logger.Red, "Exception filter: %s", err.Error())
			}
		}
	}
}

func (l *LighthouseService) Close() error {
	return l.conn.Close()
}

func (l *LighthouseService) WriteCloseMessage() error {
	return l.conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
}

func LoadPrivateKey(hexPrivateKey string) (*ecdsa.PrivateKey, error) {
	cleanKey := strings.TrimPrefix(hexPrivateKey, "0x")

	privateKey, err := crypto.HexToECDSA(cleanKey)
	if err != nil {
		return nil, err
	}
	return privateKey, nil
}

func GetSignature(target, hexPrivateKey string) ([]byte, error) {
	privateKey, err := LoadPrivateKey(hexPrivateKey)
	if err != nil {
		return nil, err
	}

	hash := crypto.Keccak256Hash(
		[]byte(target),
	)

	signature, err := crypto.Sign(hash.Bytes(), privateKey)
	if err != nil {
		return nil, err
	}
	return signature, nil
}
