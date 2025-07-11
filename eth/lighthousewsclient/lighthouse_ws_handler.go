package lighthousewsclient

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/gorilla/websocket"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/lighthousewsclient/events"
	"github.com/ledgerwatch/erigon/eth/lighthousewsclient/requests"
	"github.com/ledgerwatch/erigon/eth/lighthousewsclient/responses"
	"github.com/ledgerwatch/erigon/logger"
)

type BaseMessage struct {
	Id        *string           `json:"id"`
	EventType *events.EventType `json:"eventType"`
}

type LighthouseMessageHandler struct {
	serverConn      *websocket.Conn
	lighthouseTxsCh chan *common.LighthouseTransactions
}

func NewLighthouseMessageHandler(serverConn *websocket.Conn, lighthouseTxsCh chan *common.LighthouseTransactions) *LighthouseMessageHandler {
	return &LighthouseMessageHandler{
		serverConn:      serverConn,
		lighthouseTxsCh: lighthouseTxsCh,
	}
}

func (l *LighthouseMessageHandler) handleRollupVerifiedResponse(res *responses.RollupVerifiedResponse) error {
	logger.ColorPrintln(logger.BgCyan, "Successfully rollup verified")
	return nil
}

func (l *LighthouseMessageHandler) handleAuctionCreatedResponse(res *responses.AuctionCreatedResponse) error {
	logger.ColorPrintln(logger.Cyan, "Successfully auction Created. auctionId: "+*res.AuctionId)
	return nil
}

func (l *LighthouseMessageHandler) handleAuctionClosedEvent(event *events.AuctionClosedEvent) error {
	logger.ColorPrintln(logger.Cyan, "Successfully auction closed. auctionId: "+*event.AuctionId)
	l.lighthouseTxsCh <- &common.LighthouseTransactions{
		SlotNumber:      *event.SlotNumber,
		RawTransactions: event.RawTransactions,
	}
	return nil
}

func (l *LighthouseMessageHandler) HandleEnvelope(envelope []byte) error {
	base := new(BaseMessage)
	if err := json.Unmarshal(envelope, base); err != nil {
		return err
	}

	switch {
	case base.Id != nil:
		res := new(responses.ResponseMessage)
		if err := json.Unmarshal(envelope, res); err != nil {
			return fmt.Errorf("failed to parse response: %w", err)
		}
		if err := l.handleResponse(res); err != nil {
			return err
		}
	case base.EventType != nil:
		event := new(events.EventMessage)
		if err := json.Unmarshal(envelope, event); err != nil {
			return fmt.Errorf("failed to parse event: %w", err)
		}
		if err := l.handleEvent(event); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown message format")
	}
	return nil
}

func (l *LighthouseMessageHandler) handleResponse(res *responses.ResponseMessage) error {
	if res.Error != nil {
		return fmt.Errorf("[ErrorResponse] id=%s type=%s msg=%s", res.Id, res.ResponseType, res.Error.Message)
	}

	switch res.ResponseType {
	case responses.RollupVerified:
		payload := new(responses.RollupVerifiedResponse)
		if err := json.Unmarshal(res.Payload, payload); err != nil {
			return fmt.Errorf("failed to decode RollupVerifiedMessage: %w", err)
		}
		return l.handleRollupVerifiedResponse(payload)
	case responses.AuctionCreated:
		payload := new(responses.AuctionCreatedResponse)
		if err := json.Unmarshal(res.Payload, payload); err != nil {
			return fmt.Errorf("failed to decode AuctionCreatedMessage: %w", err)
		}
		return l.handleAuctionCreatedResponse(payload)
	default:
		return fmt.Errorf("unknown response message type")
	}
}

func (l *LighthouseMessageHandler) handleEvent(event *events.EventMessage) error {
	switch event.EventType {
	case events.AuctionClosed:
		var payload *events.AuctionClosedEvent
		if err := json.Unmarshal(event.Payload, payload); err != nil {
			return fmt.Errorf("failed to decode AuctionClosedEvent: %w", err)
		}
		return l.handleAuctionClosedEvent(payload)
	default:
		return fmt.Errorf("unknown event type")
	}
}

func (l *LighthouseMessageHandler) SendMessage(requestType requests.RequestType, params requests.RequestParams) error {
	payload, err := params.Marshal()
	if err != nil {
		return fmt.Errorf("failed to serialize message: %w", err)
	}

	id := uuid.New().String()
	requestMessage := &requests.RequestMessage{
		Id:          id,
		RequestType: requestType,
		Payload:     payload,
	}
	data, err := json.Marshal(requestMessage)
	if err != nil {
		return fmt.Errorf("failed to wrap message: %w", err)
	}
	return l.serverConn.WriteMessage(websocket.BinaryMessage, data)
}
