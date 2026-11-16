package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net/http"
	"strconv"
	"time"
)

// MockAPIService hosts simulated external APIs for Freee Accounting and Chatwork.
type MockAPIService struct {
	DefaultDelay time.Duration
}

// NewMockAPIService instantiates the mock APIs with the configured baseline delay.
func NewMockAPIService(defaultDelayMs int) *MockAPIService {
	if defaultDelayMs <= 0 {
		defaultDelayMs = 150
	}
	return &MockAPIService{
		DefaultDelay: time.Duration(defaultDelayMs) * time.Millisecond,
	}
}

// ============================================================================
// Freee Accounting Mock API
// Endpoint: POST /mock/freee/expense
// ============================================================================

// FreeeExpensePayload represents the payload expected by the Freee expense API.
type FreeeExpensePayload struct {
	IssueDate       string  `json:"issue_date"`
	Type            string  `json:"type"`
	Amount          float64 `json:"amount"`
	Currency        string  `json:"currency"`
	PartnerName     string  `json:"partner_name"`
	ExpenseCategory string  `json:"expense_category"`
	Description     string  `json:"description"`
}

// FreeeExpenseResponse represents the response returned by Freee.
type FreeeExpenseResponse struct {
	Status    string  `json:"status"`
	System    string  `json:"system"`
	DealID    int64   `json:"deal_id"`
	Amount    float64 `json:"amount"`
	Currency  string  `json:"currency"`
	Partner   string  `json:"partner"`
	SyncedAt  string  `json:"synced_at"`
}

func (s *MockAPIService) HandleFreeeExpense(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// 1. Process simulated delays or timeout test flags
	s.applySimulatedLatencyOrTimeout(r)

	// Check if simulated 500 error was requested
	if r.URL.Query().Get("fail") == "true" || r.Header.Get("X-Simulate-Fail") == "true" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "Freee API Internal Server Error (simulated)",
			"code":   "FREEE_UPSTREAM_FAULT",
			"system": "freee_accounting",
		})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "unable to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload FreeeExpensePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "malformed JSON body",
		})
		return
	}

	// Validation
	if payload.Amount <= 0 || payload.PartnerName == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusUnprocessableEntity)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "amount must be positive and partner_name is required",
		})
		return
	}

	// Generate synthetic deal ID
	dealID, _ := rand.Int(rand.Reader, big.NewInt(9000000))
	dealIDVal := dealID.Int64() + 1000000

	resp := FreeeExpenseResponse{
		Status:   "success",
		System:   "freee_accounting",
		DealID:   dealIDVal,
		Amount:   payload.Amount,
		Currency: payload.Currency,
		Partner:  payload.PartnerName,
		SyncedAt: time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[MockFreee] Error writing response: %v", err)
	}
}

// ============================================================================
// Chatwork Notification Mock API
// Endpoint: POST /mock/chatwork/message
// ============================================================================

// ChatworkMessagePayload represents the payload sent to Chatwork's room message endpoint.
type ChatworkMessagePayload struct {
	RoomID  string `json:"room_id"`
	Body    string `json:"body"`
	TraceID string `json:"trace_id,omitempty"`
}

// ChatworkMessageResponse represents Chatwork's response object.
type ChatworkMessageResponse struct {
	Status      string `json:"status"`
	System      string `json:"system"`
	MessageID   string `json:"message_id"`
	RoomID      string `json:"room_id"`
	DeliveredAt string `json:"delivered_at"`
}

func (s *MockAPIService) HandleChatworkMessage(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error": "method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// 1. Process simulated delays or timeout test flags
	s.applySimulatedLatencyOrTimeout(r)

	// Check if simulated 500 error was requested
	if r.URL.Query().Get("fail") == "true" || r.Header.Get("X-Simulate-Fail") == "true" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadGateway)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error":  "Chatwork Gateway Failure (simulated)",
			"code":   "CHATWORK_DISPATCH_TIMEOUT",
			"system": "chatwork",
		})
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, `{"error": "unable to read request body"}`, http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	var payload ChatworkMessagePayload
	if err := json.Unmarshal(body, &payload); err != nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "malformed JSON body",
		})
		return
	}

	if payload.Body == "" {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"error": "message body cannot be empty",
		})
		return
	}

	// Generate synthetic message ID
	msgBytes := make([]byte, 4)
	_, _ = rand.Read(msgBytes)
	msgID := fmt.Sprintf("cw_msg_%s", hex.EncodeToString(msgBytes))

	room := payload.RoomID
	if room == "" {
		room = "accounting-notifications"
	}

	resp := ChatworkMessageResponse{
		Status:      "success",
		System:      "chatwork",
		MessageID:   msgID,
		RoomID:      room,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		log.Printf("[MockChatwork] Error writing response: %v", err)
	}
}

// ============================================================================
// Latency / Timeout Simulation Helper
// ============================================================================

// applySimulatedLatencyOrTimeout handles artificial delay injection.
// If "?simulate_timeout=true" or header "X-Simulate-Timeout: true" is present,
// it sleeps for 6 seconds (which exceeds the 5-second client context timeout).
func (s *MockAPIService) applySimulatedLatencyOrTimeout(r *http.Request) {
	// Check for deliberate timeout injection
	if r.URL.Query().Get("simulate_timeout") == "true" || r.Header.Get("X-Simulate-Timeout") == "true" {
		log.Printf("[MockAPI] Deliberate timeout simulation triggered for %s. Sleeping 6.0s (exceeds 5.0s client deadline)...", r.URL.Path)
		select {
		case <-time.After(6 * time.Second):
			return
		case <-r.Context().Done():
			// Client cancelled/timed out as expected
			return
		}
	}

	// Custom delay query param: ?delay_ms=500 or header X-Mock-Delay: 500
	delay := s.DefaultDelay
	if dStr := r.URL.Query().Get("delay_ms"); dStr != "" {
		if dVal, err := strconv.Atoi(dStr); err == nil && dVal >= 0 {
			delay = time.Duration(dVal) * time.Millisecond
		}
	} else if dHeader := r.Header.Get("X-Mock-Delay"); dHeader != "" {
		if dVal, err := strconv.Atoi(dHeader); err == nil && dVal >= 0 {
			delay = time.Duration(dVal) * time.Millisecond
		}
	}

	if delay > 0 {
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
		}
	}
}
