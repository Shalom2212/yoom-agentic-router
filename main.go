package main

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	// 1. Load environment variables from .env if present
	loadDotEnv(".env")

	// 2. Read configuration
	cfg := loadConfig()

	// 3. Initialize core services
	llmClient := NewLLMClient(cfg)
	router := NewRouter(cfg, llmClient)
	mockAPIs := NewMockAPIService(cfg.MockDelayMillis)

	// 4. Configure HTTP routes using Go 1.22+ method-based ServeMux patterns
	mux := http.NewServeMux()

	// Ingress Webhook endpoint
	mux.HandleFunc("POST /webhook", router.HandleWebhook)

	// Downstream Mock Endpoints
	mux.HandleFunc("POST /mock/freee/expense", mockAPIs.HandleFreeeExpense)
	mux.HandleFunc("POST /mock/chatwork/message", mockAPIs.HandleChatworkMessage)

	// Health and Diagnostic endpoint
	mux.HandleFunc("GET /health", router.HandleHealth)

	// 5. Wrap multiplexer with production-grade middleware (logging, panic recovery)
	handler := recoveryMiddleware(loggingMiddleware(mux))

	// 6. Configure HTTP Server with sane timeouts
	serverAddr := ":" + cfg.Port
	server := &http.Server{
		Addr:              serverAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 7. Start server in a background goroutine
	go func() {
		printBanner(cfg)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("[FATAL] Server failed to start: %v", err)
		}
	}()

	// 8. Graceful shutdown listening for OS signals (SIGINT, SIGTERM)
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	sig := <-stopChan
	log.Printf("\n[SHUTDOWN] Received signal '%s'. Initiating graceful shutdown...", sig)

	// Allow up to 10 seconds for active goroutines and requests to drain
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("[SHUTDOWN] Server forced to shutdown: %v", err)
	}

	log.Println("[SHUTDOWN] Server shut down cleanly. Exiting.")
}

// ============================================================================
// Configuration & Environment Helpers
// ============================================================================

func loadConfig() *Config {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	llmProvider := os.Getenv("LLM_PROVIDER")
	if llmProvider == "" {
		llmProvider = "openai"
	}

	openaiKey := os.Getenv("OPENAI_API_KEY")
	openaiModel := os.Getenv("OPENAI_MODEL")
	if openaiModel == "" {
		openaiModel = "gpt-4o-mini"
	}

	anthropicKey := os.Getenv("ANTHROPIC_API_KEY")
	anthropicModel := os.Getenv("ANTHROPIC_MODEL")
	if anthropicModel == "" {
		anthropicModel = "claude-3-5-sonnet-20241022"
	}

	freeeURL := os.Getenv("FREEE_API_URL")
	if freeeURL == "" {
		freeeURL = fmt.Sprintf("http://localhost:%s/mock/freee/expense", port)
	} else if strings.Contains(freeeURL, "localhost:8080") && port != "8080" {
		freeeURL = strings.Replace(freeeURL, "localhost:8080", "localhost:"+port, 1)
	}

	chatworkURL := os.Getenv("CHATWORK_API_URL")
	if chatworkURL == "" {
		chatworkURL = fmt.Sprintf("http://localhost:%s/mock/chatwork/message", port)
	} else if strings.Contains(chatworkURL, "localhost:8080") && port != "8080" {
		chatworkURL = strings.Replace(chatworkURL, "localhost:8080", "localhost:"+port, 1)
	}

	mockDelay := 150
	if d := os.Getenv("MOCK_API_DELAY_MS"); d != "" {
		if parsed, err := strconv.Atoi(d); err == nil && parsed >= 0 {
			mockDelay = parsed
		}
	}

	return &Config{
		Port:            port,
		LLMProvider:     llmProvider,
		OpenAIKey:       openaiKey,
		OpenAIModel:     openaiModel,
		AnthropicKey:    anthropicKey,
		AnthropicModel:  anthropicModel,
		FreeeAPIURL:     freeeURL,
		ChatworkAPIURL:  chatworkURL,
		MockDelayMillis: mockDelay,
	}
}

// loadDotEnv parses key-value pairs from a .env file without external dependencies.
func loadDotEnv(filepath string) {
	file, err := os.Open(filepath)
	if err != nil {
		// Non-fatal if .env is missing; environment variables may be provided directly
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) == 2 {
			k := strings.TrimSpace(parts[0])
			v := strings.TrimSpace(parts[1])
			v = strings.Trim(v, `"'`) // strip surrounding quotes if present
			if _, exists := os.LookupEnv(k); !exists {
				_ = os.Setenv(k, v)
			}
		}
	}
}

// ============================================================================
// Standard Library Middleware
// ============================================================================

type statusRecorder struct {
	http.ResponseWriter
	statusCode int
}

func (rec *statusRecorder) WriteHeader(code int) {
	rec.statusCode = code
	rec.ResponseWriter.WriteHeader(code)
}

// loggingMiddleware captures request latency, method, path, and response status.
func loggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		recorder := &statusRecorder{ResponseWriter: w, statusCode: http.StatusOK}

		next.ServeHTTP(recorder, r)

		duration := time.Since(start)
		log.Printf("[HTTP] %-6s %-28s %3d | %7.2fms | %s",
			r.Method, r.URL.Path, recorder.statusCode, float64(duration.Microseconds())/1000.0, r.RemoteAddr)
	})
}

// recoveryMiddleware guarantees the HTTP server never crashes due to unexpected panics.
func recoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				log.Printf("[PANIC RECOVERED] %v", rec)
				writeJSONError(w, http.StatusInternalServerError, "internal server error: panic recovered", "")
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// ============================================================================
// Startup Banner
// ============================================================================

func printBanner(cfg *Config) {
	banner := `
================================================================================
  YOOM AGENTIC ROUTER - AI-Native iPaaS Webhook Integration Node
================================================================================
  Listen Address:        http://0.0.0.0:%s
  Active LLM Provider:   %s
  Freee Mock Endpoint:   %s
  Chatwork Mock Endpoint: %s
  Baseline Mock Delay:   %dms

  Available Routes (Go 1.22+ Standard ServeMux):
    POST /webhook              -> AI Expense Extraction & Parallel Fan-out
    POST /mock/freee/expense   -> Simulated Accounting Integration
    POST /mock/chatwork/message-> Simulated Notification Integration
    GET  /health               -> Health & Readiness Probe
================================================================================
`
	fmt.Printf(banner, cfg.Port, cfg.LLMProvider, cfg.FreeeAPIURL, cfg.ChatworkAPIURL, cfg.MockDelayMillis)
}
