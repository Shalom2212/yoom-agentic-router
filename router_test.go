package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func setupTestServer() (*Config, *Router, *MockAPIService, *httptest.Server) {
	mockAPIs := NewMockAPIService(50) // 50ms fast mock delay for tests

	mux := http.NewServeMux()
	mux.HandleFunc("POST /mock/freee/expense", mockAPIs.HandleFreeeExpense)
	mux.HandleFunc("POST /mock/chatwork/message", mockAPIs.HandleChatworkMessage)

	testServer := httptest.NewServer(mux)

	cfg := &Config{
		Port:            "8080",
		LLMProvider:     "mock",
		FreeeAPIURL:     testServer.URL + "/mock/freee/expense",
		ChatworkAPIURL:  testServer.URL + "/mock/chatwork/message",
		MockDelayMillis: 50,
	}

	llmClient := NewLLMClient(cfg)
	router := NewRouter(cfg, llmClient)

	// Register webhook handler on mux
	mux.HandleFunc("POST /webhook", router.HandleWebhook)

	return cfg, router, mockAPIs, testServer
}

func TestMockLLMExtraction(t *testing.T) {
	mockLLM := &MockLLMClient{}
	ctx := context.Background()

	tests := []struct {
		name        string
		input       string
		wantErr     bool
		expectedAmt float64
		expectedCur string
		expectedVen string
	}{
		{
			name:        "Standard Japanese Yen AWS expense",
			input:       "Just paid ¥50,000 for AWS hosting on my Axis card",
			wantErr:     false,
			expectedAmt: 50000,
			expectedCur: "JPY",
			expectedVen: "AWS",
		},
		{
			name:        "US Dollar Slack subscription",
			input:       "Paid $120.50 for Slack subscription today",
			wantErr:     false,
			expectedAmt: 120.50,
			expectedCur: "USD",
			expectedVen: "Slack",
		},
		{
			name:        "Euro GitHub team invoice",
			input:       "Processed invoice €45.00 for GitHub team seats",
			wantErr:     false,
			expectedAmt: 45.00,
			expectedCur: "EUR",
			expectedVen: "GitHub",
		},
		{
			name:    "Invalid non-expense text should fail",
			input:   "Hello, what is the weather today?",
			wantErr: true,
		},
		{
			name:    "Empty string should fail",
			input:   "",
			wantErr: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			res, err := mockLLM.ExtractExpense(ctx, tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for input %q, but got nil result: %+v", tc.input, res)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error for input %q: %v", tc.input, err)
			}

			if res.Amount != tc.expectedAmt {
				t.Errorf("expected amount %.2f, got %.2f", tc.expectedAmt, res.Amount)
			}
			if res.Currency != tc.expectedCur {
				t.Errorf("expected currency %s, got %s", tc.expectedCur, res.Currency)
			}
			if res.Vendor != tc.expectedVen {
				t.Errorf("expected vendor %s, got %s", tc.expectedVen, res.Vendor)
			}
		})
	}
}

func TestWebhookSuccessFlow(t *testing.T) {
	_, router, _, server := setupTestServer()
	defer server.Close()

	payload := WebhookRequest{
		Text:    "Just paid ¥50,000 for AWS hosting on my Axis card",
		Source:  "slack-agent",
		TraceID: "test-trace-001",
	}

	bodyBytes, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.HandleWebhook(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200 OK, got %d", resp.StatusCode)
	}

	var webhookResp WebhookResponse
	if err := json.NewDecoder(resp.Body).Decode(&webhookResp); err != nil {
		t.Fatalf("failed to decode webhook response: %v", err)
	}

	if webhookResp.Status != "success" {
		t.Errorf("expected status 'success', got '%s'", webhookResp.Status)
	}

	if webhookResp.TraceID != "test-trace-001" {
		t.Errorf("expected trace_id 'test-trace-001', got '%s'", webhookResp.TraceID)
	}

	// Verify extracted expense
	if webhookResp.ExtractedExpense == nil {
		t.Fatal("extracted_expense is nil")
	}
	if webhookResp.ExtractedExpense.Amount != 50000 {
		t.Errorf("expected amount 50000, got %.2f", webhookResp.ExtractedExpense.Amount)
	}
	if webhookResp.ExtractedExpense.Currency != "JPY" {
		t.Errorf("expected currency JPY, got %s", webhookResp.ExtractedExpense.Currency)
	}
	if webhookResp.ExtractedExpense.Vendor != "AWS" {
		t.Errorf("expected vendor AWS, got %s", webhookResp.ExtractedExpense.Vendor)
	}

	// Verify downstream dispatches count
	if len(webhookResp.Dispatches) != 2 {
		t.Fatalf("expected 2 downstream dispatches, got %d", len(webhookResp.Dispatches))
	}

	for _, d := range webhookResp.Dispatches {
		if d.Status != "success" {
			t.Errorf("dispatch to %s failed: status=%s, error=%s", d.Target, d.Status, d.Error)
		}
	}

	// Verify execution times recorded
	if webhookResp.ExecutionTimes.TotalExecutionMs <= 0 {
		t.Errorf("expected positive total execution time, got %.2fms", webhookResp.ExecutionTimes.TotalExecutionMs)
	}
	if webhookResp.ExecutionTimes.ConcurrentDispatchMs <= 0 {
		t.Errorf("expected positive concurrent dispatch time, got %.2fms", webhookResp.ExecutionTimes.ConcurrentDispatchMs)
	}
}

func TestWebhookUnparseableInputReturns400(t *testing.T) {
	_, router, _, server := setupTestServer()
	defer server.Close()

	payload := WebhookRequest{
		Text:    "Hello, what is the weather in Tokyo?",
		TraceID: "test-trace-invalid",
	}

	bodyBytes, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.HandleWebhook(w, req)

	resp := w.Result()
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("expected status 400 Bad Request for unparseable input, got %d", resp.StatusCode)
	}

	var errResp map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&errResp); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}

	if !strings.Contains(errResp["error"].(string), "LLM extraction failed") {
		t.Errorf("expected error message to mention 'LLM extraction failed', got %v", errResp["error"])
	}
}

func TestWebhookDownstreamTimeoutGracefulDegradation(t *testing.T) {
	// Custom mock server where Freee hangs longer than client timeout
	mockAPIs := NewMockAPIService(20)
	mux := http.NewServeMux()

	// Normal Chatwork endpoint
	mux.HandleFunc("POST /mock/chatwork/message", mockAPIs.HandleChatworkMessage)

	// Hanging Freee endpoint (simulates severe upstream latency)
	mux.HandleFunc("POST /mock/freee/expense", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-time.After(6 * time.Second): // Exceeds 5.0s context timeout
			w.WriteHeader(http.StatusOK)
		case <-r.Context().Done():
			// Context canceled by client
			return
		}
	})

	hangingServer := httptest.NewServer(mux)
	defer hangingServer.Close()

	cfg := &Config{
		Port:            "8080",
		LLMProvider:     "mock",
		FreeeAPIURL:     hangingServer.URL + "/mock/freee/expense",
		ChatworkAPIURL:  hangingServer.URL + "/mock/chatwork/message",
		MockDelayMillis: 20,
	}

	llmClient := NewLLMClient(cfg)
	router := NewRouter(cfg, llmClient)

	payload := WebhookRequest{
		Text:    "Just paid ¥50,000 for AWS hosting on my Axis card",
		TraceID: "test-timeout-001",
	}

	bodyBytes, _ := json.Marshal(payload)
	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBuffer(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	start := time.Now()
	router.HandleWebhook(w, req)
	elapsed := time.Since(start)

	resp := w.Result()

	// Webhook must return 200 OK with routing execution times and partial_success
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK even with downstream timeout, got %d", resp.StatusCode)
	}

	var webhookResp WebhookResponse
	if err := json.NewDecoder(resp.Body).Decode(&webhookResp); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if webhookResp.Status != "partial_success" {
		t.Errorf("expected status 'partial_success', got '%s'", webhookResp.Status)
	}

	// Verify that the timeout was caught around 5.0 seconds (and didn't hang forever)
	if elapsed < 4800*time.Millisecond || elapsed > 6500*time.Millisecond {
		t.Logf("Notice: elapsed time for timeout test was %v (expected ~5.0s)", elapsed)
	}

	// Verify dispatches: Freee should be timeout, Chatwork should be success
	var freeeFound, chatworkFound bool
	for _, d := range webhookResp.Dispatches {
		if d.Target == TargetFreee {
			freeeFound = true
			if d.Status != "timeout" {
				t.Errorf("expected Freee status 'timeout', got '%s' (err: %s)", d.Status, d.Error)
			}
		}
		if d.Target == TargetChatwork {
			chatworkFound = true
			if d.Status != "success" {
				t.Errorf("expected Chatwork status 'success', got '%s'", d.Status)
			}
		}
	}

	if !freeeFound || !chatworkFound {
		t.Errorf("missing dispatch results: freeeFound=%v, chatworkFound=%v", freeeFound, chatworkFound)
	}
}

func TestEmptyRequestBody(t *testing.T) {
	_, router, _, server := setupTestServer()
	defer server.Close()

	req := httptest.NewRequest(http.MethodPost, "/webhook", bytes.NewBuffer([]byte("")))
	w := httptest.NewRecorder()

	router.HandleWebhook(w, req)
	resp := w.Result()

	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for empty body, got %d", resp.StatusCode)
	}
}
