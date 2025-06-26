package main

import (
	"errors"
	"fmt"

	"gofr.dev/pkg/gofr"
	"gofr.dev/pkg/gofr/service"
)

func main() {
	// 1) Create the app
	app := gofr.New()

	// 2) Register an external HTTP service with rate limiting:
	//    – 1 request per second
	//    – burst up to 2
	//    – non‐blocking mode (excess calls fail immediately)
	app.AddHTTPService(
		"demo",
		"https://httpbin.org", // public echo service
		&service.RateLimiterConfig{
			RequestsPerSecond: 1,
			Burst:             2,
			Blocking:          false,
		},
	)

	// 3) Expose an endpoint that calls it rapidly
	app.GET("/test", func(c *gofr.Context) (any, error) {
  svc := c.GetHTTPService("demo")
  results := []string{}

  for i := 1; i <= 5; i++ {
    resp, err := svc.Get(c, "get", nil)  // this returns immediately
    if errors.Is(err, service.ErrRateLimited) {
      results = append(results, fmt.Sprintf("call %d: rate limited", i))
    } else if err != nil {
      return nil, err
    } else {
      results = append(results, fmt.Sprintf("call %d: OK", i))
      resp.Body.Close()
    }
  }
  return results, nil
})

	// 4) Run!
	app.Run()
}
