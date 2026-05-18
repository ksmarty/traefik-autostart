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
    <title>Service Starting...</title>
    <meta http-equiv="refresh" content="2">
    <style>
        body { font-family: system-ui, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #f5f5f5; }
        .container { text-align: center; padding: 2rem; background: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.1); }
        .spinner { width: 40px; height: 40px; border: 3px solid #e5e5e5; border-top-color: #3b82f6; border-radius: 50%; animation: spin 1s linear infinite; margin: 0 auto 1rem; }
        @keyframes spin { to { transform: rotate(360deg); } }
        h1 { margin: 0 0 0.5rem; color: #1f2937; }
        p { margin: 0; color: #6b7280; }
    </style>
</head>
<body>
    <div class="container">
        <div class="spinner"></div>
        <h1>Starting `+host+`</h1>
        <p>Please wait...</p>
    </div>
</body>
</html>`)
}

type WakeRequest struct {
	Host string `json:"host"`
}