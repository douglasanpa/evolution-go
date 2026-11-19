package webhook_producer

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// newTestProducer builds the producer with the real env readers, so the tests
// exercise the same path the server uses (only the delays are shortened).
func newTestProducer(t *testing.T, url string) *webhookProducer {
	t.Helper()
	producer := NewWebhookProducer(url, nil).(*webhookProducer)
	producer.retryDelays = []time.Duration{time.Millisecond, time.Millisecond}
	return producer
}

func TestSendWebhookWithRetrySucceedsOnThirdAttempt(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	producer := newTestProducer(t, server.URL)
	producer.sendWebhookWithRetry(server.URL, []byte(`{"event":"Message"}`), "instance")

	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expected 3 attempts (2 failures then success), got %d", got)
	}
}

func TestSendWebhookWithRetryDoesNotRetry400(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusBadRequest)
	}))
	defer server.Close()

	producer := newTestProducer(t, server.URL)
	producer.sendWebhookWithRetry(server.URL, []byte(`{}`), "instance")

	if got := atomic.LoadInt32(&attempts); got != 1 {
		t.Fatalf("400 must not be retried: expected 1 attempt, got %d", got)
	}
}

func TestSendWebhookWithRetryRetries429(t *testing.T) {
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&attempts, 1) < 3 {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	producer := newTestProducer(t, server.URL)
	producer.sendWebhookWithRetry(server.URL, []byte(`{}`), "instance")

	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("429 must be retried: expected 3 attempts (2 waits of 1ms), got %d", got)
	}
}

func TestWebhookTimeoutCutsHungCall(t *testing.T) {
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Receiver accepts the connection and never answers.
		<-release
	}))
	defer func() {
		close(release)
		server.Close()
	}()

	t.Setenv("WEBHOOK_TIMEOUT_SECONDS", "1")
	producer := NewWebhookProducer(server.URL, nil).(*webhookProducer)
	if producer.client.Timeout != time.Second {
		t.Fatalf("expected 1s client timeout from WEBHOOK_TIMEOUT_SECONDS, got %v", producer.client.Timeout)
	}

	start := time.Now()
	err, _, _, retryable := producer.sendWebhook(server.URL, []byte(`{}`), "instance")
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error from the hung receiver")
	}
	if !retryable {
		t.Fatal("a network/timeout error must be retryable")
	}
	if elapsed > 5*time.Second {
		t.Fatalf("the timeout did not cut the call: took %v", elapsed)
	}
}

func TestDefaultDelaysPreserveUpstreamBehavior(t *testing.T) {
	t.Setenv("WEBHOOK_TIMEOUT_SECONDS", "")
	producer := NewWebhookProducer("http://example.invalid", nil).(*webhookProducer)
	if producer.client.Timeout != 10*time.Second {
		t.Fatalf("default timeout must be 10s, got %v", producer.client.Timeout)
	}
	if len(producer.retryDelays) != 4 {
		t.Fatalf("upstream behavior is 5 attempts (4 waits of 30s), got delays %v", producer.retryDelays)
	}
	for i, d := range producer.retryDelays {
		if d != 30*time.Second {
			t.Fatalf("wait %d: expected 30s, got %v", i, d)
		}
	}
}

func TestRetryDelaysComeFromEnvironment(t *testing.T) {
	t.Setenv("WEBHOOK_RETRY_DELAYS", "1ms,1ms")
	producer := newTestProducer(t, "http://example.invalid")

	want := []time.Duration{time.Millisecond, time.Millisecond}
	if len(producer.retryDelays) != len(want) {
		t.Fatalf("expected %d delays, got %v", len(want), producer.retryDelays)
	}
	for i := range want {
		if producer.retryDelays[i] != want[i] {
			t.Fatalf("delay %d: expected %v, got %v", i, want[i], producer.retryDelays[i])
		}
	}

	// Number of attempts = len(delays) + 1.
	var attempts int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&attempts, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()
	producer.sendWebhookWithRetry(server.URL, []byte(`{}`), "instance")
	if got := atomic.LoadInt32(&attempts); got != 3 {
		t.Fatalf("expected len(delays)+1 = 3 attempts, got %d", got)
	}
}

func TestRetryDelaysInvalidValueFallsBackToDefault(t *testing.T) {
	t.Setenv("WEBHOOK_RETRY_DELAYS", "5s,banana")
	producer := NewWebhookProducer("http://example.invalid", nil).(*webhookProducer)
	if len(producer.retryDelays) != 4 || producer.retryDelays[0] != 30*time.Second {
		t.Fatalf("invalid value must fall back to the 4x30s default, got %v", producer.retryDelays)
	}
}

func TestTruncateForLogCapsResponseBody(t *testing.T) {
	long := strings.Repeat("a", 500)
	got := truncateForLog([]byte(long))
	if len([]rune(got)) > maxLoggedResponseBody+len("...(truncated)") {
		t.Fatalf("log body not truncated: %d chars", len(got))
	}
	if !strings.HasSuffix(got, "...(truncated)") {
		t.Fatalf("expected truncation marker, got %q", got)
	}
	if truncateForLog([]byte("ok")) != "ok" {
		t.Fatal("short body must be kept as is")
	}
}
