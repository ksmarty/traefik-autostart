// Package traefik_container_sleep is a Traefik plugin that wakes Docker containers on request.
package traefik_container_sleep

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	defaultTimeout = 30 // seconds
	defaultURL     = "http://controller:5000/wake"
)

// Config is the plugin configuration.
type Config struct {
	// Timeout is the maximum seconds to wait for the controller to wake a container.
	Timeout int    `json:"timeout"`
	URL     string `json:"url"`
}

// CreateConfig returns the default plugin configuration.
func CreateConfig() *Config {
	return &Config{
		Timeout: defaultTimeout,
		URL:     defaultURL,
	}
}

// AutoStart is the plugin middleware.
type AutoStart struct {
	next    http.Handler
	timeout time.Duration
	url     string
	name    string
}

// New creates a new AutoStart middleware instance.
func New(ctx context.Context, next http.Handler, config *Config, name string) (http.Handler, error) {
	return &AutoStart{
		next:    next,
		timeout: time.Duration(config.Timeout) * time.Second,
		url:     config.URL,
		name:    name,
	}, nil
}

func (a *AutoStart) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	host := req.Host
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	payload := WakeRequest{
		Host: host,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		a.serveLandingPage(rw, host)
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), a.timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, strings.NewReader(string(body)))
	if err != nil {
		a.serveLandingPage(rw, host)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: a.timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		a.serveLandingPage(rw, host)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		rw.Header().Set("X-Wake-Status", "ready")
		a.next.ServeHTTP(rw, req)
		return
	}

	a.serveLandingPage(rw, host)
}

func (a *AutoStart) serveLandingPage(rw http.ResponseWriter, host string) {
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.WriteHeader(http.StatusServiceUnavailable)
	fmt.Fprint(rw, `<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Starting `+host+`</title>
    <style>
        *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
        body {
            font-family: system-ui, -apple-system, sans-serif;
            display: flex;
            align-items: center;
            justify-content: center;
            min-height: 100vh;
            background: #f3f4f6;
        }
        .card {
            text-align: center;
            padding: 2.5rem 3rem;
            background: #fff;
            border-radius: 12px;
            box-shadow: 0 4px 24px rgba(0,0,0,0.08);
        }
        .spinner {
            width: 44px;
            height: 44px;
            border: 3px solid #e5e7eb;
            border-top-color: #3b82f6;
            border-radius: 50%;
            animation: spin 0.75s linear infinite;
            margin: 0 auto 1.5rem;
        }
        @keyframes spin { to { transform: rotate(360deg); } }
        h1 { font-size: 1.1rem; font-weight: 600; color: #111827; margin-bottom: 0.4rem; }
        p  { font-size: 0.9rem; color: #6b7280; }
    </style>
</head>
<body>
    <div class="card">
        <div class="spinner"></div>
        <h1>`+host+`</h1>
        <p>Starting up&hellip;</p>
    </div>
    <script>
        // Poll the current URL every 2 seconds. When the response is no longer a 503
        // the container is up and Traefik has registered the backend — reload the page.
        (function () {
            setInterval(function () {
                fetch(window.location.href, { credentials: 'include' })
                    .then(function (r) {
                        if (r.status !== 503) {
                            window.location.reload();
                        }
                    })
                    .catch(function () { /* network error — keep waiting */ });
            }, 2000);
        }());
    </script>
</body>
</html>`)
}

type WakeRequest struct {
	Host string `json:"host"`
}