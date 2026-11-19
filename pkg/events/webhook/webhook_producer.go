package webhook_producer

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	producer_interfaces "github.com/evolution-foundation/evolution-go/pkg/events/interfaces"
	logger_wrapper "github.com/evolution-foundation/evolution-go/pkg/logger"
)

// Defaults for webhook delivery. The upstream hard-coded 5 attempts with a
// fixed 30 s wait: if the receiver was down for ~2.5 min the event was lost,
// and the call had no timeout, so a hung receiver kept the goroutine forever.
const (
	defaultWebhookTimeoutSeconds = 10
	// Upstream behavior preserved: 5 attempts, 30 s between them (4 waits).
	defaultWebhookRetryDelays = "30s,30s,30s,30s"
	// maxLoggedResponseBody caps what goes to the log: the response may echo
	// message content, which is personal data (LGPD).
	maxLoggedResponseBody = 200
	// maxResponseBodyBytes caps what is read from the response.
	maxResponseBodyBytes = 64 * 1024
)

type webhookProducer struct {
	url           string
	loggerWrapper *logger_wrapper.LoggerManager
	client        *http.Client
	retryDelays   []time.Duration
}

func NewWebhookProducer(
	url string,
	loggerWrapper *logger_wrapper.LoggerManager,
) producer_interfaces.Producer {
	return &webhookProducer{
		url:           url,
		loggerWrapper: loggerWrapper,
		client:        &http.Client{Timeout: webhookTimeout()},
		retryDelays:   webhookRetryDelays(loggerWrapper),
	}
}

// webhookTimeout reads WEBHOOK_TIMEOUT_SECONDS (seconds, default 10).
func webhookTimeout() time.Duration {
	raw := strings.TrimSpace(os.Getenv("WEBHOOK_TIMEOUT_SECONDS"))
	if raw == "" {
		return defaultWebhookTimeoutSeconds * time.Second
	}
	seconds, err := strconv.Atoi(raw)
	if err != nil || seconds <= 0 {
		return defaultWebhookTimeoutSeconds * time.Second
	}
	return time.Duration(seconds) * time.Second
}

// webhookRetryDelays reads WEBHOOK_RETRY_DELAYS (a comma-separated list of
// time.Duration values, e.g. "5s,15s,1m"). The number of attempts is
// len(delays)+1. An invalid value falls back to the default and is reported in
// the log.
func webhookRetryDelays(loggerWrapper *logger_wrapper.LoggerManager) []time.Duration {
	raw := strings.TrimSpace(os.Getenv("WEBHOOK_RETRY_DELAYS"))
	if raw == "" {
		return mustParseDelays(defaultWebhookRetryDelays)
	}
	delays, err := parseRetryDelays(raw)
	if err != nil {
		logForManager(loggerWrapper, "webhook", func(logger *logger_wrapper.Logger) {
			logger.LogWarn("[webhook] invalid WEBHOOK_RETRY_DELAYS %q (%v), using default %q", raw, err, defaultWebhookRetryDelays)
		})
		return mustParseDelays(defaultWebhookRetryDelays)
	}
	return delays
}

func parseRetryDelays(raw string) ([]time.Duration, error) {
	parts := strings.Split(raw, ",")
	delays := make([]time.Duration, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			return nil, fmt.Errorf("empty delay in %q", raw)
		}
		d, err := time.ParseDuration(part)
		if err != nil {
			return nil, err
		}
		if d < 0 {
			return nil, fmt.Errorf("negative delay %q", part)
		}
		delays = append(delays, d)
	}
	if len(delays) == 0 {
		return nil, fmt.Errorf("no delays in %q", raw)
	}
	return delays, nil
}

// logForManager runs fn with the named logger when a manager is available.
func logForManager(loggerWrapper *logger_wrapper.LoggerManager, name string, fn func(*logger_wrapper.Logger)) {
	if loggerWrapper == nil {
		return
	}
	if logger := loggerWrapper.GetLogger(name); logger != nil {
		fn(logger)
	}
}

func mustParseDelays(raw string) []time.Duration {
	delays, err := parseRetryDelays(raw)
	if err != nil {
		// Only reachable if the constant itself is wrong.
		return []time.Duration{5 * time.Second}
	}
	return delays
}

// logFor returns the logger for the instance. Without a manager (unit tests)
// no log is written, but the retry path is still exercised.
func (p *webhookProducer) logFor(userID string) *logger_wrapper.Logger {
	if p.loggerWrapper != nil {
		return p.loggerWrapper.GetLogger(userID)
	}
	return nil
}

func (p *webhookProducer) logInfo(userID, format string, args ...interface{}) {
	if logger := p.logFor(userID); logger != nil {
		logger.LogInfo(format, args...)
	}
}

func (p *webhookProducer) logWarn(userID, format string, args ...interface{}) {
	if logger := p.logFor(userID); logger != nil {
		logger.LogWarn(format, args...)
	}
}

func (p *webhookProducer) logError(userID, format string, args ...interface{}) {
	if logger := p.logFor(userID); logger != nil {
		logger.LogError(format, args...)
	}
}

func (p *webhookProducer) Produce(
	queueName string,
	payload []byte,
	webhookUrl string,
	userID string,
) error {
	splitQueue := strings.Split(queueName, ".")

	if len(splitQueue) < 2 {
		return nil
	}

	if p.url != "" {
		go p.sendWebhookWithRetry(p.url, payload, userID)
	}
	if webhookUrl != "" {
		go p.sendWebhookWithRetry(webhookUrl, payload, userID)
	}

	return nil
}

func (p *webhookProducer) sendWebhookWithRetry(url string, body []byte, userID string) {
	attempts := len(p.retryDelays) + 1
	for attempt := 1; attempt <= attempts; attempt++ {
		err, responseBody, statusCode, retryable := p.sendWebhook(url, body, userID)
		if err == nil {
			p.logInfo(userID, "[%s] webhook sent successfully - url: %s, status: %d, response: %s", userID, url, statusCode, truncateForLog(responseBody))
			return
		}

		p.logWarn(userID, "[%s] webhook failed - url: %s, attempt: %d/%d, status: %d, error: %v, response: %s",
			userID, url, attempt, attempts, statusCode, err, truncateForLog(responseBody))

		if !retryable {
			// A 4xx (other than 408/429) will not change on retry: the receiver
			// rejected the payload. Retrying only makes noise.
			p.logError(userID, "[%s] webhook not retried (non-retryable status %d) - url: %s", userID, statusCode, url)
			return
		}

		if attempt == attempts {
			break
		}
		time.Sleep(p.retryDelays[attempt-1])
	}
	p.logError(userID, "[%s] webhook failed after maximum retries - url: %s", userID, url)
}

// sendWebhook posts the payload once. retryable is false only for a response a
// retry cannot fix: 4xx except 408 (timeout) and 429 (rate limited).
func (p *webhookProducer) sendWebhook(url string, body []byte, userID string) (error, []byte, int, bool) {
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(body))
	if err != nil {
		return err, nil, 0, false
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := p.client.Do(req)
	if err != nil {
		return err, nil, 0, true
	}
	defer resp.Body.Close()

	responseBody, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodyBytes))
	if err != nil {
		return fmt.Errorf("erro ao ler resposta: %v", err), nil, 0, true
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return nil, responseBody, resp.StatusCode, true
	}

	retryable := resp.StatusCode < 400 || resp.StatusCode >= 500 ||
		resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests
	return errors.New("received non-2xx response: " + resp.Status), responseBody, resp.StatusCode, retryable
}

// truncateForLog limits what is written to the log: the response body may carry
// personal data (message text), so only the first 200 characters are kept.
func truncateForLog(body []byte) string {
	text := strings.TrimSpace(string(body))
	if text == "" {
		return "-"
	}
	runes := []rune(text)
	if len(runes) > maxLoggedResponseBody {
		return string(runes[:maxLoggedResponseBody]) + "...(truncated)"
	}
	return text
}

// CreateGlobalQueues nao faz nada para webhook producer
func (p *webhookProducer) CreateGlobalQueues() error {
	return nil
}
