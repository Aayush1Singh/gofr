package service

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/time/rate"
)

var ErrRateLimited = errors.New("rate limit exceeded")
type RateLimiterConfig struct {
    RequestsPerSecond float64 // RequestsPerSecond is number of request allowed per second.
    Burst int // Burst is the maximum burst size allowed.
	Blocking bool //default: false, used to toggle between Wait and Allow.
}
type rateLimitedHTTP struct {
    next        HTTP
    limiter     *rate.Limiter
    serviceName string
    logger      Logger
    metrics     Metrics
    tracer      trace.Tracer
	blocking    bool
}
func (rl *RateLimiterConfig) AddOption(inner HTTP) HTTP {
    limiter := rate.NewLimiter(
        rate.Limit(rl.RequestsPerSecond),
        rl.Burst,
    )
    base, ok := inner.(*httpService)
    if !ok {
        return inner
    }


    return &rateLimitedHTTP{
        next:        base,
        limiter:     limiter,
        serviceName: base.url,
        logger:      base.Logger,
        metrics:     base.Metrics,
        tracer:      base.Tracer,
		blocking:	 rl.Blocking,
		}
}
func (r *rateLimitedHTTP) Name() string {
    return r.serviceName
}
func (r *rateLimitedHTTP) Get(ctx context.Context, path string, params map[string]interface{}) (*http.Response, error) {
    var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    // allowed: forward the call
    return r.next.Get(ctx, path, params)
}


func (r *rateLimitedHTTP) GetWithHeaders(ctx context.Context, path string, params map[string]interface{},headers map[string]string) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.GetWithHeaders(ctx, path, params,headers)
}
func (r *rateLimitedHTTP) Post(ctx context.Context, path string, params map[string]interface{},body []byte) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.Post(ctx, path, params, body)
}
func (r *rateLimitedHTTP) PostWithHeaders(ctx context.Context, path string, params map[string]any,body []byte,headers map[string]string) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.PostWithHeaders(ctx, path, params,body,headers)
}
func (r *rateLimitedHTTP) Patch(ctx context.Context, path string, params map[string]interface{},body []byte) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.Patch(ctx, path, params, body)
}
func (r *rateLimitedHTTP) PatchWithHeaders(ctx context.Context, path string, params map[string]interface{},body []byte,headers map[string]string) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.PatchWithHeaders(ctx, path, params, body,headers)
}
func (r *rateLimitedHTTP) Delete(ctx context.Context, path string,body []byte) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.Delete(ctx, path, body)
}
func (r *rateLimitedHTTP) Put(ctx context.Context, path string,params map[string]any,body []byte) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.Put(ctx, path,params, body)
}
func (r *rateLimitedHTTP) PutWithHeaders(ctx context.Context, path string,params map[string]any,body []byte,headers map[string]string) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.PutWithHeaders(ctx, path,params, body,headers)
}
func (r *rateLimitedHTTP) DeleteWithHeaders(ctx context.Context, path string,body []byte, params map[string]string) (*http.Response, error) {
        var ok bool
    if r.blocking {
        // blocking mode: wait for a token (or return context error)
        err := r.limiter.Wait(ctx)
        if err != nil {
            return nil, err
        }
        ok = true
    } else {
        // non-blocking mode: immediate check
        ok = r.limiter.Allow()
    }

    if !ok {
        // 1) Log
        r.logger.Log(
            "level", "WARN",
            "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
        )

        // 2) Trace event
        span := trace.SpanFromContext(ctx)
        span.AddEvent(
            "rate_limited",
            trace.WithAttributes(attribute.String("service", r.serviceName)),
        )

        // 3) Metric
        r.metrics.RecordHistogram(
            ctx,
            "http_service_rate_limited",
            1.0,
            "service", r.serviceName,
        )

        return nil, ErrRateLimited
    }
    return r.next.DeleteWithHeaders(ctx, path, body,params)
}

func (r *rateLimitedHTTP) HealthCheck(ctx context.Context) *Health {
    // HealthChecks shouldn’t be rate limited—
    // just delegate straight through.
    return r.next.HealthCheck(ctx)
}
func (r *rateLimitedHTTP) getHealthResponseForEndpoint(ctx context.Context, endpoint string, timeout int) *Health {
    // HealthChecks shouldn’t be rate limited—
    // just delegate straight through.
    return r.HealthCheck(ctx)
}

// Sample Code for using without blocking
// func (r *rateLimitedHTTP) Allow(ctx context.Context) (error) {
//     if !r.limiter.Allow() {
//         // 1) Log — just dump a formatted message via Logger.Log
//         r.logger.Log(
//             "level", "WARN",
//             "msg", fmt.Sprintf("rate limit hit for service %q", r.serviceName),
//         )
//         // 2) Trace event
//                 span := trace.SpanFromContext(ctx)
//         span.AddEvent(
//             "rate_limited",
//             trace.WithAttributes(attribute.String("service", r.serviceName)),
//         )
//         // **METRIC** record a histogram observation of “1”
//         r.metrics.RecordHistogram(
//             ctx,
//             "http_service_rate_limited",
//             1.0,
//             "service", r.serviceName,
//         )

//         return  ErrRateLimited
//     }
//     return nil
// }

// func (r *rateLimitedHTTP) Get(ctx context.Context, path string, params map[string]interface{}) (*http.Response, error) {
//     if err := r.Allow(ctx); err != nil {
//         return nil, err
//     }
//     return r.next.Get(ctx, path, params)
// }