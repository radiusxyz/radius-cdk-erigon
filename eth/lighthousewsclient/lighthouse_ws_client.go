package lighthousewsclient

import (
	"crypto/ecdsa"
	"encoding/base64"
	"fmt"
	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/lighthousewsclient/requests"
	"github.com/ledgerwatch/erigon/logger"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type LighthouseWsClient struct {
	*ethconfig.Config
	conn       *websocket.Conn
	leaveCh    chan struct{}
	envelopeCh chan []byte
	handler    *LighthouseMessageHandler
}

func NewLighthouseWsClient(config *ethconfig.Config, lighthouseTxCh chan *common.LighthouseTransactions) (*LighthouseWsClient, error) {
	timestamp := uint64(time.Now().Unix())
	signature, err := GetSignature(config.RollupId, timestamp, config.SequencerPrivateKey)
	if err != nil {
		panic(err)
	}

	headers := http.Header{}
	headers.Set("ClientType", "Rollup")
	headers.Set("RollupId", config.RollupId)
	headers.Set("Signature", base64.StdEncoding.EncodeToString(signature))
	headers.Set("Timestamp", strconv.FormatUint(timestamp, 10))

	conn, _, err := websocket.DefaultDialer.Dial(config.LighthouseUrl, headers)
	if err != nil {
		return nil, err
	}

	return &LighthouseWsClient{
		Config:     config,
		conn:       conn,
		leaveCh:    make(chan struct{}),
		envelopeCh: make(chan []byte),
		handler:    NewLighthouseMessageHandler(conn, lighthouseTxCh),
	}, nil
}

func (l *LighthouseWsClient) Start() {
	for i := 0; i < 1; i++ {
		go l.ManageCh()
	}

	go l.ReadMessage()

	timestamp := uint64(time.Now().Unix())
	signature, err := GetSignature(l.RollupId, timestamp, l.SequencerPrivateKey)
	if err != nil {
		panic(err)
	}

	verifyRollupRequest := &requests.VerifyRollupRequest{
		RollupId:  l.RollupId,
		Timestamp: timestamp,
		Signature: signature,
	}

	if err = l.handler.SendMessage(requests.VerifyRollup, verifyRollupRequest); err != nil {
		logger.Println("Write error:", err)
	}

	logger.Printf("rollup(%s) verification message sent", l.RollupId)
}

func (l *LighthouseWsClient) CreateAuction(slotNumber int64, slotTime uint64) (*uint64, error) {
	auctionStartTimestamp := uint64(time.Now().Unix())
	createAuctionRequest := &requests.CreateAuctionRequest{
		RollupId:              l.RollupId,
		SlotNumber:            slotNumber,
		SlotTime:              slotTime,
		AuctionStartTimestamp: auctionStartTimestamp,
	}
	requestType := requests.CreateAuction
	if err := l.handler.SendMessage(requestType, createAuctionRequest); err != nil {
		return nil, err
	}

	logger.ColorPrintln(logger.BrightCyan, "Sent auction creation message. slotNumber: "+strconv.FormatInt(slotNumber, 10))

	return &auctionStartTimestamp, nil
}

func (l *LighthouseWsClient) ReadMessage() {
	for {
		_, message, err := l.conn.ReadMessage()
		if err != nil {
			logger.Println("Read error:", err)
			l.leaveCh <- struct{}{}
			break
		}
		l.envelopeCh <- message
	}
}

func (l *LighthouseWsClient) ManageCh() {
	for {
		select {
		case <-l.leaveCh:
			_ = l.conn.Close()
			logger.Println("Connection to the server has been lost")
			l.Reconnect()

		case envelope := <-l.envelopeCh:
			if err := l.handler.HandleEnvelope(envelope); err != nil {
				logger.ColorPrintf(logger.Red, "Exception filter: %s", err.Error())
			}
		}
	}
}

func (l *LighthouseWsClient) resetConn(conn *websocket.Conn) {
	l.conn = conn
	l.handler.ResetConn(conn)
}

func (l *LighthouseWsClient) Reconnect() {
	for {
		time.Sleep(time.Second * 5)
		conn, _, err := websocket.DefaultDialer.Dial(l.Config.LighthouseUrl, nil)
		if err != nil {
			logger.ColorPrintf(logger.Red, "Dial error: %s", err.Error())
			continue
		}
		l.resetConn(conn)
		go l.ReadMessage()
		break
	}
}

func (l *LighthouseWsClient) Close() error {
	return l.conn.Close()
}

func (l *LighthouseWsClient) WriteCloseMessage() error {
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

func GetSignature(target string, timestamp uint64, hexPrivateKey string) ([]byte, error) {
	privateKey, err := LoadPrivateKey(hexPrivateKey)
	if err != nil {
		return nil, err
	}

	message := fmt.Sprintf("%s|%d", target, timestamp)

	hash := crypto.Keccak256Hash(
		[]byte(message),
	)

	signature, err := crypto.Sign(hash.Bytes(), privateKey)
	if err != nil {
		return nil, err
	}
	return signature, nil
}
