package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// WebhookRequest is the incoming payload sent to POST /webhook.
type WebhookRequest struct {
	Text    string `json:"text"`
	Source  string `json:"source,omitempty"`
	TraceID string `json:"trace_id,omitempty"`
}

// DispatchTarget identifies which downstream system was targeted.
type DispatchTarget string

const (
	TargetFreee    DispatchTarget = "freee_accounting"
	TargetChatwork DispatchTarget = "chatwork_notification"
)

// DispatchResult captures the outcome and timing of a single downstream API call.
type DispatchResult struct {
	Target       DispatchTarget `json:"target"`
	Endpoint     string         `json:"endpoint"`
	Status       string         `json:"status"` // "success", "timeout", "failed"
	StatusCode   int            `json:"status_code,omitempty"`
	DurationMs   float64        `json:"duration_ms"`
	ResponseData any            `json:"response_data,omitempty"`
	Error        string         `json:"error,omitempty"`
}

// RoutingExecutionTimes captures fine-grained timing metrics for the iPaaS pipeline.
type RoutingExecutionTimes struct {
	LLMExtractionMs      float64 `json:"llm_extraction_ms"`
	FreeeDispatchMs      float64 `json:"freee_dispatch_ms"`
	ChatworkDispatchMs   float64 `json:"chatwork_dispatch_ms"`
	ConcurrentDispatchMs float64 `json:"concurrent_dispatch_ms"`
	TotalExecutionMs     float64 `json:"total_execution_ms"`
}

// WebhookResponse is the 200 OK response returning extraction and routing metrics.
type WebhookResponse struct {
	Status           string                `json:"status"` // "success", "partial_success", "downstream_failure"
	Message          string                `json:"message"`
	TraceID          string                `json:"trace_id"`
	ExtractedExpense *ExtractedExpense     `json:"extracted_expense"`
	ExecutionTimes   RoutingExecutionTimes `json:"execution_times"`
	Dispatches       []DispatchResult      `json:"dispatches"`
}

// Router orchestrates the AI extraction and concurrent downstream API dispatches.
type Router struct {
	cfg        *Config
	llmClient  LLMClient
	httpClient *http.Client
}

// NewRouter instantiates the router with tuned connection pooling for high-concurrency workloads.
func NewRouter(cfg *Config, llmClient LLMClient) *Router {
	// Custom transport with HTTP keep-alives and pooling optimized for high-concurrency microservices
	transport := &http.Transport{
		MaxIdleConns:        1000,
		MaxIdleConnsPerHost: 200,
		MaxConnsPerHost:     500,
		IdleConnTimeout:     90 * time.Second,
		DisableKeepAlives:   false,
	}

	return &Router{
		cfg:       cfg,
		llmClient: llmClient,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   10 * time.Second, // Context timeout governs the explicit 5-second deadline
		},
	}
}

// HandleWebhook processes POST /webhook requests.
func (ro *Router) HandleWebhook(w http.ResponseWriter, r *http.Request) {
	totalStart := time.Now()

	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed: POST required", "")
		return
	}

	// 1. Read and parse incoming webhook body
	bodyBytes, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1024*1024)) // limit to 1MB
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "failed to read request body or body too large", "")
		return
	}
	defer r.Body.Close()

	var req WebhookRequest
	if err := json.Unmarshal(bodyBytes, &req); err != nil {
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("malformed JSON payload: %v", err), "")
		return
	}

	if strings.TrimSpace(req.Text) == "" {
		writeJSONError(w, http.StatusBadRequest, "field 'text' cannot be empty", req.TraceID)
		return
	}

	// Assign or preserve trace ID for request tracing
	traceID := req.TraceID
	if traceID == "" {
		traceID = generateTraceID()
	}

	log.Printf("[Webhook] [%s] Ingested webhook payload: %q (source: %s)", traceID, req.Text, req.Source)

	// 2. AI Extraction with strict 5-second context timeout
	llmCtx, llmCancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer llmCancel()

	llmStart := time.Now()
	extracted, err := ro.llmClient.ExtractExpense(llmCtx, req.Text)
	llmDuration := time.Since(llmStart)
	llmDurationMs := float64(llmDuration.Microseconds()) / 1000.0

	if err != nil {
		log.Printf("[Webhook] [%s] LLM extraction error after %.2fms: %v", traceID, llmDurationMs, err)
		// Return 400 Bad Request if LLM fails to parse
		writeJSONError(w, http.StatusBadRequest, fmt.Sprintf("LLM extraction failed: %v", err), traceID)
		return
	}

	log.Printf("[Webhook] [%s] Successfully extracted expense: Amount=%.2f %s, Vendor=%s, Category=%s (took %.2fms)",
		traceID, extracted.Amount, extracted.Currency, extracted.Vendor, extracted.Category, llmDurationMs)

	// 3. Concurrent API Dispatch using Goroutines, Channels, and sync.WaitGroup
	// Create buffered channel of capacity 2 to avoid any risk of goroutine leaks
	resultChan := make(chan DispatchResult, 2)
	var wg sync.WaitGroup
	wg.Add(2)

	dispatchStart := time.Now()

	// Dispatch 1: Mock Accounting API (Freee)
	go func() {
		defer wg.Done()
		callCtx, callCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer callCancel()

		result := ro.dispatchToFreee(callCtx, extracted, traceID, r)
		resultChan <- result
	}()

	// Dispatch 2: Mock Notification API (Chatwork)
	go func() {
		defer wg.Done()
		callCtx, callCancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer callCancel()

		result := ro.dispatchToChatwork(callCtx, extracted, traceID, r)
		resultChan <- result
	}()

	// Wait for concurrent dispatches to complete
	wg.Wait()
	close(resultChan)

	concurrentDuration := time.Since(dispatchStart)
	concurrentDurationMs := float64(concurrentDuration.Microseconds()) / 1000.0

	// 4. Aggregate results from the channel
	var dispatches []DispatchResult
	var freeeDurationMs, chatworkDurationMs float64
	hasTimeout := false
	hasFailure := false

	for res := range resultChan {
		dispatches = append(dispatches, res)
		switch res.Target {
		case TargetFreee:
			freeeDurationMs = res.DurationMs
		case TargetChatwork:
			chatworkDurationMs = res.DurationMs
		}

		if res.Status == "timeout" {
			hasTimeout = true
		} else if res.Status == "failed" {
			hasFailure = true
		}
	}

	totalDuration := time.Since(totalStart)
	totalDurationMs := float64(totalDuration.Microseconds()) / 1000.0

	// Determine overall status
	status := "success"
	message := "Expense successfully extracted and dispatched to Freee and Chatwork"
	if hasTimeout {
		status = "partial_success"
		message = "Expense extracted, but one or more downstream dispatches timed out"
	} else if hasFailure {
		status = "partial_success"
		message = "Expense extracted, but one or more downstream dispatches encountered an error"
	}

	response := WebhookResponse{
		Status:           status,
		Message:          message,
		TraceID:          traceID,
		ExtractedExpense: extracted,
		ExecutionTimes: RoutingExecutionTimes{
			LLMExtractionMs:      llmDurationMs,
			FreeeDispatchMs:      freeeDurationMs,
			ChatworkDispatchMs:   chatworkDurationMs,
			ConcurrentDispatchMs: concurrentDurationMs,
			TotalExecutionMs:     totalDurationMs,
		},
		Dispatches: dispatches,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(response); err != nil {
		log.Printf("[Webhook] [%s] Error writing response: %v", traceID, err)
	}

	log.Printf("[Webhook] [%s] Completed routing in %.2fms (LLM: %.2fms, Concurrent Dispatch: %.2fms, Status: %s)",
		traceID, totalDurationMs, llmDurationMs, concurrentDurationMs, status)
}

// ============================================================================
// Downstream Dispatch Functions
// ============================================================================

func (ro *Router) dispatchToFreee(ctx context.Context, expense *ExtractedExpense, traceID string, originalReq *http.Request) DispatchResult {
	start := time.Now()
	freeeURL := ro.cfg.FreeeAPIURL

	// Forward any simulation headers for timeout or error testing
	reqPayload := FreeeExpensePayload{
		IssueDate:       time.Now().Format("2006-01-02"),
		Type:            "expense",
		Amount:          expense.Amount,
		Currency:        expense.Currency,
		PartnerName:     expense.Vendor,
		ExpenseCategory: expense.Category,
		Description:     fmt.Sprintf("Auto-routed by yoom-agentic-router [%s]: %s (%s)", traceID, expense.Vendor, expense.Category),
	}

	jsonBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return DispatchResult{
			Target:     TargetFreee,
			Endpoint:   freeeURL,
			Status:     "failed",
			DurationMs: elapsedMs(start),
			Error:      fmt.Sprintf("failed to marshal Freee payload: %v", err),
		}
	}

	// Propagate simulation query parameters if present (e.g. ?simulate_timeout=freee)
	targetURL := freeeURL
	if originalReq.URL.Query().Get("simulate_timeout") == "freee" || originalReq.URL.Query().Get("simulate_timeout") == "all" {
		targetURL += "?simulate_timeout=true"
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return DispatchResult{
			Target:     TargetFreee,
			Endpoint:   targetURL,
			Status:     "failed",
			DurationMs: elapsedMs(start),
			Error:      fmt.Sprintf("failed to create Freee request: %v", err),
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Trace-ID", traceID)
	// Forward test simulation headers if present
	if h := originalReq.Header.Get("X-Simulate-Timeout-Freee"); h != "" {
		httpReq.Header.Set("X-Simulate-Timeout", h)
	} else if h := originalReq.Header.Get("X-Simulate-Timeout"); h != "" {
		httpReq.Header.Set("X-Simulate-Timeout", h)
	}

	resp, err := ro.httpClient.Do(httpReq)
	durationMs := elapsedMs(start)

	if err != nil {
		// Gracefully handle timeout without crashing the server
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) || ctx.Err() == context.DeadlineExceeded {
			log.Printf("[Dispatch] [Freee] [Timeout] %s after %.2fms: %v", targetURL, durationMs, err)
			return DispatchResult{
				Target:     TargetFreee,
				Endpoint:   targetURL,
				Status:     "timeout",
				DurationMs: durationMs,
				Error:      fmt.Sprintf("Freee API timed out after 5.0s: %v", err),
			}
		}

		log.Printf("[Dispatch] [Freee] [Failed] %s after %.2fms: %v", targetURL, durationMs, err)
		return DispatchResult{
			Target:     TargetFreee,
			Endpoint:   targetURL,
			Status:     "failed",
			DurationMs: durationMs,
			Error:      fmt.Sprintf("Freee API network error: %v", err),
		}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var parsedResp any
	_ = json.Unmarshal(respBody, &parsedResp)

	status := "success"
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		status = "failed"
	}

	return DispatchResult{
		Target:       TargetFreee,
		Endpoint:     targetURL,
		Status:       status,
		StatusCode:   resp.StatusCode,
		DurationMs:   durationMs,
		ResponseData: parsedResp,
	}
}

func (ro *Router) dispatchToChatwork(ctx context.Context, expense *ExtractedExpense, traceID string, originalReq *http.Request) DispatchResult {
	start := time.Now()
	chatworkURL := ro.cfg.ChatworkAPIURL

	formattedBody := fmt.Sprintf("[info][title]Yoom Auto-Expense Ingested[/title]Vendor: %s\nAmount: %s %.2f\nCategory: %s\nTrace: %s[/info]",
		expense.Vendor, expense.Currency, expense.Amount, expense.Category, traceID)

	reqPayload := ChatworkMessagePayload{
		RoomID:  "accounting-notifications",
		Body:    formattedBody,
		TraceID: traceID,
	}

	jsonBytes, err := json.Marshal(reqPayload)
	if err != nil {
		return DispatchResult{
			Target:     TargetChatwork,
			Endpoint:   chatworkURL,
			Status:     "failed",
			DurationMs: elapsedMs(start),
			Error:      fmt.Sprintf("failed to marshal Chatwork payload: %v", err),
		}
	}

	// Propagate simulation query parameters if present (e.g. ?simulate_timeout=chatwork)
	targetURL := chatworkURL
	if originalReq.URL.Query().Get("simulate_timeout") == "chatwork" || originalReq.URL.Query().Get("simulate_timeout") == "all" {
		targetURL += "?simulate_timeout=true"
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, targetURL, bytes.NewBuffer(jsonBytes))
	if err != nil {
		return DispatchResult{
			Target:     TargetChatwork,
			Endpoint:   targetURL,
			Status:     "failed",
			DurationMs: elapsedMs(start),
			Error:      fmt.Sprintf("failed to create Chatwork request: %v", err),
		}
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-Trace-ID", traceID)
	// Forward test simulation headers if present
	if h := originalReq.Header.Get("X-Simulate-Timeout-Chatwork"); h != "" {
		httpReq.Header.Set("X-Simulate-Timeout", h)
	} else if h := originalReq.Header.Get("X-Simulate-Timeout"); h != "" {
		httpReq.Header.Set("X-Simulate-Timeout", h)
	}

	resp, err := ro.httpClient.Do(httpReq)
	durationMs := elapsedMs(start)

	if err != nil {
		// Gracefully handle timeout without crashing the server
		if errors.Is(err, context.DeadlineExceeded) || os.IsTimeout(err) || ctx.Err() == context.DeadlineExceeded {
			log.Printf("[Dispatch] [Chatwork] [Timeout] %s after %.2fms: %v", targetURL, durationMs, err)
			return DispatchResult{
				Target:     TargetChatwork,
				Endpoint:   targetURL,
				Status:     "timeout",
				DurationMs: durationMs,
				Error:      fmt.Sprintf("Chatwork API timed out after 5.0s: %v", err),
			}
		}

		log.Printf("[Dispatch] [Chatwork] [Failed] %s after %.2fms: %v", targetURL, durationMs, err)
		return DispatchResult{
			Target:     TargetChatwork,
			Endpoint:   targetURL,
			Status:     "failed",
			DurationMs: durationMs,
			Error:      fmt.Sprintf("Chatwork API network error: %v", err),
		}
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var parsedResp any
	_ = json.Unmarshal(respBody, &parsedResp)

	status := "success"
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		status = "failed"
	}

	return DispatchResult{
		Target:       TargetChatwork,
		Endpoint:     targetURL,
		Status:       status,
		StatusCode:   resp.StatusCode,
		DurationMs:   durationMs,
		ResponseData: parsedResp,
	}
}

// ============================================================================
// Health & Utility Handlers
// ============================================================================

// HandleHealth provides a simple readiness probe for the service.
func (ro *Router) HandleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status":    "healthy",
		"service":   "yoom-agentic-router",
		"timestamp": time.Now().UTC().Format(time.RFC3339),
	})
}

func writeJSONError(w http.ResponseWriter, statusCode int, message, traceID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error":    message,
		"code":     statusCode,
		"trace_id": traceID,
	})
}

func elapsedMs(start time.Time) float64 {
	return float64(time.Since(start).Microseconds()) / 1000.0
}

func generateTraceID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("req-%s", hex.EncodeToString(b))
}
