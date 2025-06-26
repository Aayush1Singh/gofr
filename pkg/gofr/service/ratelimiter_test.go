package service

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

//––– Stubs –––//

type dummyHTTP struct {
	called bool
}

func (d *dummyHTTP) Name() string { return "dummy" }

func (d *dummyHTTP) Get(ctx context.Context, path string, params map[string]any) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (d *dummyHTTP) GetWithHeaders(ctx context.Context, path string, params map[string]any, headers map[string]string) (*http.Response, error) {
	d.called = true
	return d.Get(ctx, path, params)
}

func (d *dummyHTTP) Post(ctx context.Context, path string, params map[string]any, body []byte) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (d *dummyHTTP) PostWithHeaders(ctx context.Context, path string, params map[string]any, body []byte, headers map[string]string) (*http.Response, error) {
	d.called = true
	return d.Post(ctx, path, params, body)
}

func (d *dummyHTTP) Put(ctx context.Context, path string, params map[string]any, body []byte) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (d *dummyHTTP) PutWithHeaders(ctx context.Context, path string, params map[string]any, body []byte, headers map[string]string) (*http.Response, error) {
	d.called = true
	return d.Put(ctx, path, params, body)
}

func (d *dummyHTTP) Patch(ctx context.Context, path string, params map[string]any, body []byte) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (d *dummyHTTP) PatchWithHeaders(ctx context.Context, path string, params map[string]any, body []byte, headers map[string]string) (*http.Response, error) {
	d.called = true
	return d.Patch(ctx, path, params, body)
}

func (d *dummyHTTP) Delete(ctx context.Context, path string, body []byte) (*http.Response, error) {
	d.called = true
	return &http.Response{StatusCode: http.StatusOK}, nil
}

func (d *dummyHTTP) DeleteWithHeaders(ctx context.Context, path string, body []byte, headers map[string]string) (*http.Response, error) {
	d.called = true
	return d.Delete(ctx, path, body)
}

func (d *dummyHTTP) HealthCheck(context.Context) *Health {
	return nil
}
func (r *dummyHTTP) getHealthResponseForEndpoint(ctx context.Context, endpoint string, timeout int) *Health {
    // HealthChecks shouldn’t be rate limited—
    // just delegate straight through.
    return r.HealthCheck(ctx)
}
// dummyLogger is a no-op logger
type dummyLogger struct{}

func (l *dummyLogger) Log(args ...interface{}) {}

// dummyMetrics is a no-op metrics collector
type dummyMetrics struct{}

func (m *dummyMetrics) RecordHistogram(ctx context.Context, name string, value float64, labels ...string) {}

//––– Tests –––//

func fireBurst(t *testing.T, r *rateLimitedHTTP, burst int) {
	for i := 0; i < burst; i++ {
		resp, err := r.Get(context.Background(), "/", nil)
		if err != nil {
			t.Fatalf("unexpected error on attempt %d: %v", i+1, err)
		}
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("expected 200 OK on attempt %d, got %d", i+1, resp.StatusCode)
		}
	}
}

func TestNonBlockingWithinBurst(t *testing.T) {
	rc := RateLimiterConfig{RequestsPerSecond: 2, Burst: 3, Blocking: false}
	lim := rate.NewLimiter(rate.Limit(rc.RequestsPerSecond), rc.Burst)
	r := &rateLimitedHTTP{
		next:        &dummyHTTP{},
		limiter:     lim,
		serviceName: "svc",
		logger:      &dummyLogger{},
		metrics:     &dummyMetrics{},
		tracer:      nil,
		blocking:    rc.Blocking,
	}

	fireBurst(t, r, rc.Burst)
}

func TestNonBlockingRejectsAfterBurst(t *testing.T) {
	rc := RateLimiterConfig{RequestsPerSecond: 1, Burst: 1, Blocking: false}
	lim := rate.NewLimiter(rate.Limit(rc.RequestsPerSecond), rc.Burst)
	r := &rateLimitedHTTP{
		next:        &dummyHTTP{},
		limiter:     lim,
		serviceName: "svc",
		logger:      &dummyLogger{},
		metrics:     &dummyMetrics{},
		tracer:      nil,
		blocking:    rc.Blocking,
	}

	if _, err := r.Get(context.Background(), "/", nil); err != nil {
		t.Fatalf("expected first request allowed, got %v", err)
	}
	_, err := r.Get(context.Background(), "/", nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited, got %v", err)
	}
}

func TestBlockingWaits(t *testing.T) {
	rc := RateLimiterConfig{RequestsPerSecond: 1, Burst: 1, Blocking: true}
	lim := rate.NewLimiter(rate.Limit(rc.RequestsPerSecond), rc.Burst)
	r := &rateLimitedHTTP{
		next:        &dummyHTTP{},
		limiter:     lim,
		serviceName: "svc",
		logger:      &dummyLogger{},
		metrics:     &dummyMetrics{},
		tracer:      nil,
		blocking:    rc.Blocking,
	}

	ctx := context.Background()
	start := time.Now()

	if _, err := r.Get(ctx, "/", nil); err != nil {
		t.Fatalf("first request failed: %v", err)
	}
	if _, err := r.Get(ctx, "/", nil); err != nil {
		t.Fatalf("second request failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("expected wait ≥ 1s, got %v", elapsed)
	}
}

func TestZeroRPSRejectsAll(t *testing.T) {
	rc := RateLimiterConfig{RequestsPerSecond: 0, Burst: 0, Blocking: false}
	lim := rate.NewLimiter(rate.Limit(rc.RequestsPerSecond), rc.Burst)
	r := &rateLimitedHTTP{
		next:        &dummyHTTP{},
		limiter:     lim,
		serviceName: "svc",
		logger:      &dummyLogger{},
		metrics:     &dummyMetrics{},
		tracer:      nil,
		blocking:    rc.Blocking,
	}

	_, err := r.Get(context.Background(), "/", nil)
	if !errors.Is(err, ErrRateLimited) {
		t.Fatalf("expected ErrRateLimited on zero-RPS, got %v", err)
	}
}

func TestContextTimeoutCancelsBlocking(t *testing.T) {
	rc := RateLimiterConfig{RequestsPerSecond: 0.5, Burst: 1, Blocking: true}
	lim := rate.NewLimiter(rate.Limit(rc.RequestsPerSecond), rc.Burst)

	r := &rateLimitedHTTP{
		next:        &dummyHTTP{},
		limiter:     lim,
		serviceName: "svc",
		logger:      &dummyLogger{},
		metrics:     &dummyMetrics{},
		tracer:      nil,
		blocking:    rc.Blocking,
	}
	// Consume the one available token
	_, err := r.Get(context.Background(), "/", nil)
	if err != nil {
		t.Fatalf("unexpected error on first request: %v", err)
	}

	// Second request should block, and the context will timeout before limiter allows it
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	_, err = r.Get(ctx, "/", nil)
	t.Logf("Returned error: %v", err)
	if err == nil {
		t.Fatal("expected DeadlineExceeded or rate limiter timeout error, got nil")
	}

	if !errors.Is(err, context.DeadlineExceeded) && err.Error() != "rate: Wait(n=1) would exceed context deadline" {
		t.Fatalf("expected context.DeadlineExceeded or rate limiter timeout error, got %v", err)
	}
}


