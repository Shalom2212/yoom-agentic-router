package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// ErrUnparseableExpense is returned when the input text does not represent a valid expense.
var ErrUnparseableExpense = errors.New("unparseable expense: input text does not contain valid expense data")

// ExtractedExpense represents the structured financial data extracted from unstructured text.
type ExtractedExpense struct {
	Amount   float64 `json:"amount"`
	Currency string  `json:"currency"`
	Vendor   string  `json:"vendor"`
	Category string  `json:"category"`
}

// Validate checks whether the extracted expense contains necessary non-zero fields.
func (e *ExtractedExpense) Validate() error {
	if e == nil {
		return fmt.Errorf("%w: nil expense", ErrUnparseableExpense)
	}
	if e.Amount <= 0 {
		return fmt.Errorf("%w: invalid or non-positive amount (%.2f)", ErrUnparseableExpense, e.Amount)
	}
	if strings.TrimSpace(e.Currency) == "" {
		return fmt.Errorf("%w: missing currency", ErrUnparseableExpense)
	}
	if strings.TrimSpace(e.Vendor) == "" {
		return fmt.Errorf("%w: missing vendor", ErrUnparseableExpense)
	}
	if strings.TrimSpace(e.Category) == "" {
		return fmt.Errorf("%w: missing category", ErrUnparseableExpense)
	}
	return nil
}

// LLMClient defines the interface for AI text extraction.
type LLMClient interface {
	ExtractExpense(ctx context.Context, text string) (*ExtractedExpense, error)
}

// Config holds runtime settings for LLM extraction and downstream routing.
type Config struct {
	Port            string
	LLMProvider     string
	OpenAIKey       string
	OpenAIModel     string
	AnthropicKey    string
	AnthropicModel  string
	FreeeAPIURL     string
	ChatworkAPIURL  string
	MockDelayMillis int
}

// OpenAIClient implements LLMClient using OpenAI's Chat Completions API.
type OpenAIClient struct {
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

// AnthropicClient implements LLMClient using Anthropic's Messages API.
type AnthropicClient struct {
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

// MockLLMClient provides an intelligent local fallback parser for offline testing.
type MockLLMClient struct{}

// NewLLMClient constructs the appropriate LLM client based on configuration.
func NewLLMClient(cfg *Config) LLMClient {
	provider := strings.ToLower(strings.TrimSpace(cfg.LLMProvider))

	// If OpenAI credentials are valid and selected
	if provider == "openai" && cfg.OpenAIKey != "" && !strings.HasPrefix(cfg.OpenAIKey, "your_") {
		log.Printf("[LLM] Initialized live OpenAI client (model: %s)", cfg.OpenAIModel)
		return &OpenAIClient{
			APIKey: cfg.OpenAIKey,
			Model:  cfg.OpenAIModel,
			HTTPClient: &http.Client{
				Timeout: 10 * time.Second, // Context timeout controls the actual deadline (5s)
			},
		}
	}

	// If Anthropic credentials are valid and selected
	if provider == "anthropic" && cfg.AnthropicKey != "" && !strings.HasPrefix(cfg.AnthropicKey, "your_") {
		log.Printf("[LLM] Initialized live Anthropic client (model: %s)", cfg.AnthropicModel)
		return &AnthropicClient{
			APIKey: cfg.AnthropicKey,
			Model:  cfg.AnthropicModel,
			HTTPClient: &http.Client{
				Timeout: 10 * time.Second,
			},
		}
	}

	// Default to high-fidelity mock extraction engine for local demonstration
	log.Printf("[LLM] No valid API key detected for provider '%s'. Operating in intelligent MOCK LLM mode.", provider)
	return &MockLLMClient{}
}

// ============================================================================
// OpenAI Implementation
// ============================================================================

type openAIChatRequest struct {
	Model          string              `json:"model"`
	Messages       []openAIChatMessage `json:"messages"`
	Temperature    float64             `json:"temperature"`
	ResponseFormat *openAIRespFormat   `json:"response_format,omitempty"`
}

type openAIChatMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openAIRespFormat struct {
	Type string `json:"type"`
}

type openAIChatResponse struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
		Type    string `json:"type"`
	} `json:"error,omitempty"`
}

func (c *OpenAIClient) ExtractExpense(ctx context.Context, text string) (*ExtractedExpense, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("%w: empty input text", ErrUnparseableExpense)
	}

	reqBody := openAIChatRequest{
		Model: c.Model,
		Messages: []openAIChatMessage{
			{
				Role: "system",
				Content: "You are an expense extraction engine for an automated enterprise iPaaS webhook. " +
					"Extract the expense details from user input and return strictly valid JSON matching this schema: " +
					`{"amount": <number>, "currency": "<ISO 3-letter code or recognized symbol like JPY, USD, EUR>", "vendor": "<vendor/merchant name>", "category": "<concise category e.g. Hosting, Software, Travel, Meals>"} ` +
					"If the text does NOT describe an expense, return {\"error\": \"not_an_expense\"}. No Markdown backticks.",
			},
			{
				Role:    "user",
				Content: text,
			},
		},
		Temperature:    0.1,
		ResponseFormat: &openAIRespFormat{Type: "json_object"},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal OpenAI request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.openai.com/v1/chat/completions", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create OpenAI HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Authorization", "Bearer "+c.APIKey)

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("OpenAI API request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read OpenAI response body: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("OpenAI API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var chatResp openAIChatResponse
	if err := json.Unmarshal(bodyBytes, &chatResp); err != nil {
		return nil, fmt.Errorf("failed to decode OpenAI response: %w", err)
	}

	if len(chatResp.Choices) == 0 {
		return nil, fmt.Errorf("%w: OpenAI returned empty choices", ErrUnparseableExpense)
	}

	content := strings.TrimSpace(chatResp.Choices[0].Message.Content)
	return parseExtractedJSON(content)
}

// ============================================================================
// Anthropic Implementation
// ============================================================================

type anthropicRequest struct {
	Model     string             `json:"model"`
	MaxTokens int                `json:"max_tokens"`
	System    string             `json:"system"`
	Messages  []anthropicMessage `json:"messages"`
}

type anthropicMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type anthropicResponse struct {
	Content []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"content"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

func (c *AnthropicClient) ExtractExpense(ctx context.Context, text string) (*ExtractedExpense, error) {
	if strings.TrimSpace(text) == "" {
		return nil, fmt.Errorf("%w: empty input text", ErrUnparseableExpense)
	}

	reqBody := anthropicRequest{
		Model:     c.Model,
		MaxTokens: 500,
		System: "You are an expense extraction engine for an iPaaS webhook router. " +
			"Extract the transaction details and reply STRICTLY with a JSON object: " +
			`{"amount": <number>, "currency": "<ISO code or symbol>", "vendor": "<vendor name>", "category": "<category>"}. ` +
			"If the text is not an expense, return {\"error\": \"not_an_expense\"}. Output raw JSON only with no markdown wrapping.",
		Messages: []anthropicMessage{
			{Role: "user", Content: text},
		},
	}

	jsonData, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal Anthropic request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://api.anthropic.com/v1/messages", bytes.NewBuffer(jsonData))
	if err != nil {
		return nil, fmt.Errorf("failed to create Anthropic HTTP request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("x-api-key", c.APIKey)
	httpReq.Header.Set("anthropic-version", "2023-06-01")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("Anthropic API request failed: %w", err)
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read Anthropic response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("Anthropic API returned status %d: %s", resp.StatusCode, string(bodyBytes))
	}

	var anthropicResp anthropicResponse
	if err := json.Unmarshal(bodyBytes, &anthropicResp); err != nil {
		return nil, fmt.Errorf("failed to decode Anthropic response: %w", err)
	}

	if len(anthropicResp.Content) == 0 {
		return nil, fmt.Errorf("%w: Anthropic returned empty content", ErrUnparseableExpense)
	}

	content := strings.TrimSpace(anthropicResp.Content[0].Text)
	return parseExtractedJSON(content)
}

// ============================================================================
// Mock LLM Implementation (Heuristic parser for zero-credential testability)
// ============================================================================

func (m *MockLLMClient) ExtractExpense(ctx context.Context, text string) (*ExtractedExpense, error) {
	// Respect context cancellation / timeout
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	clean := strings.TrimSpace(text)
	if clean == "" {
		return nil, fmt.Errorf("%w: empty text", ErrUnparseableExpense)
	}

	// Reject obvious non-expense queries (enables testing 400 Bad Request)
	lower := strings.ToLower(clean)
	nonExpensePatterns := []string{"hello", "weather", "who are you", "what time is it", "ping"}
	for _, p := range nonExpensePatterns {
		if strings.Contains(lower, p) && !strings.Contains(lower, "paid") && !strings.Contains(lower, "¥") && !strings.Contains(lower, "$") {
			return nil, fmt.Errorf("%w: '%s' is not an expense payment description", ErrUnparseableExpense, text)
		}
	}

	// 1. Currency & Amount extraction
	var currency string = "JPY"
	var amount float64

	// Match currency symbols and numbers: ¥50,000, $120.50, €45.00, £30, 50000 JPY, 100 USD
	reAmount := regexp.MustCompile(`([¥$€£]|JPY|USD|EUR|GBP|INR)?\s*([0-9]{1,3}(?:,[0-9]{3})*(?:\.[0-9]+)?|[0-9]+(?:\.[0-9]+)?)\s*([¥$€£]|JPY|USD|EUR|GBP|INR)?`)
	matches := reAmount.FindAllStringSubmatch(clean, -1)

	foundAmount := false
	for _, match := range matches {
		rawSymbol := match[1]
		if rawSymbol == "" {
			rawSymbol = match[3]
		}
		rawNum := strings.ReplaceAll(match[2], ",", "")
		parsedVal, err := strconv.ParseFloat(rawNum, 64)
		if err == nil && parsedVal > 0 {
			amount = parsedVal
			foundAmount = true
			switch rawSymbol {
			case "¥", "JPY":
				currency = "JPY"
			case "$", "USD":
				currency = "USD"
			case "€", "EUR":
				currency = "EUR"
			case "£", "GBP":
				currency = "GBP"
			case "INR":
				currency = "INR"
			default:
				if strings.Contains(clean, "¥") {
					currency = "JPY"
				} else if strings.Contains(clean, "$") {
					currency = "USD"
				} else if strings.Contains(clean, "€") {
					currency = "EUR"
				} else {
					currency = "JPY" // default for Japanese business contexts
				}
			}
			break
		}
	}

	if !foundAmount {
		return nil, fmt.Errorf("%w: unable to detect monetary amount in '%s'", ErrUnparseableExpense, text)
	}

	// 2. Vendor extraction
	vendor := "Unknown Vendor"
	knownVendors := []string{
		"AWS", "Amazon Web Services", "GCP", "Google Cloud", "Microsoft Azure", "Azure",
		"Slack", "GitHub", "GitLab", "Zoom", "Notion", "Stripe", "Apple", "OpenAI",
		"Figma", "Datadog", "Atlassian", "Jira", "Salesforce", "HubSpot", "Cloudflare",
	}

	for _, v := range knownVendors {
		reVendor := regexp.MustCompile(`(?i)\b` + regexp.QuoteMeta(v) + `\b`)
		if reVendor.MatchString(clean) {
			vendor = v
			break
		}
	}

	// Fallback vendor detection: "for <Vendor> hosting" or "at <Vendor>"
	if vendor == "Unknown Vendor" {
		reForVendor := regexp.MustCompile(`(?i)(?:for|at|to)\s+([A-Za-z0-9_-]+)`)
		if m := reForVendor.FindStringSubmatch(clean); len(m) > 1 {
			vendor = strings.Title(strings.ToLower(m[1]))
		} else {
			return nil, fmt.Errorf("%w: unable to identify vendor in text", ErrUnparseableExpense)
		}
	}

	// 3. Category extraction
	category := "General Business Expense"
	switch {
	case strings.Contains(lower, "hosting") || strings.Contains(lower, "cloud") || strings.Contains(lower, "server") || strings.Contains(lower, "infrastructure"):
		category = "Hosting & Infrastructure"
	case strings.Contains(lower, "software") || strings.Contains(lower, "saas") || strings.Contains(lower, "subscription") || strings.Contains(lower, "license"):
		category = "Software & Subscriptions"
	case strings.Contains(lower, "meal") || strings.Contains(lower, "dinner") || strings.Contains(lower, "lunch") || strings.Contains(lower, "coffee") || strings.Contains(lower, "food"):
		category = "Meals & Entertainment"
	case strings.Contains(lower, "travel") || strings.Contains(lower, "flight") || strings.Contains(lower, "hotel") || strings.Contains(lower, "taxi") || strings.Contains(lower, "uber"):
		category = "Travel & Lodging"
	case strings.Contains(lower, "office") || strings.Contains(lower, "supplies") || strings.Contains(lower, "desk"):
		category = "Office Supplies"
	case strings.Contains(lower, "ads") || strings.Contains(lower, "marketing") || strings.Contains(lower, "google ads") || strings.Contains(lower, "facebook ads"):
		category = "Advertising & Marketing"
	}

	expense := &ExtractedExpense{
		Amount:   amount,
		Currency: currency,
		Vendor:   vendor,
		Category: category,
	}

	if err := expense.Validate(); err != nil {
		return nil, err
	}

	return expense, nil
}

// ============================================================================
// Helpers
// ============================================================================

func parseExtractedJSON(raw string) (*ExtractedExpense, error) {
	// Strip markdown code fences if LLM wrapped output in ```json ... ```
	trimmed := strings.TrimSpace(raw)
	if strings.HasPrefix(trimmed, "```") {
		lines := strings.Split(trimmed, "\n")
		if len(lines) >= 2 {
			if strings.HasPrefix(lines[0], "```") {
				lines = lines[1:]
			}
			if len(lines) > 0 && strings.HasPrefix(lines[len(lines)-1], "```") {
				lines = lines[:len(lines)-1]
			}
			trimmed = strings.TrimSpace(strings.Join(lines, "\n"))
		}
	}

	// Check if LLM explicitly declared error / not an expense
	var errCheck struct {
		Error string `json:"error"`
	}
	if err := json.Unmarshal([]byte(trimmed), &errCheck); err == nil && errCheck.Error != "" {
		return nil, fmt.Errorf("%w: %s", ErrUnparseableExpense, errCheck.Error)
	}

	var expense ExtractedExpense
	if err := json.Unmarshal([]byte(trimmed), &expense); err != nil {
		return nil, fmt.Errorf("%w: failed to parse JSON from LLM: %v", ErrUnparseableExpense, err)
	}

	if err := expense.Validate(); err != nil {
		return nil, err
	}

	return &expense, nil
}
