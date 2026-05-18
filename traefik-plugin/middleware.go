package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

const (
	defaultTimeout = 30 * time.Second
	defaultURL      = "http://controller:5000/wake"
)

type Config struct {
	Timeout time.Duration `json:"timeout"`
	URL     string        `json:"url"`
}

func New(ctx context.Context, config map[string]interface{}) (interface{}, error) {
	cfg := Config{
		Timeout: defaultTimeout,
		URL:     defaultURL,
	}

	if timeout, ok := config["timeout"].(string); ok {
		d, err := time.ParseDuration(timeout)
		if err != nil {
			return nil, fmt.Errorf("invalid timeout: %w", err)
		}
		cfg.Timeout = d
	}

	if url, ok := config["url"].(string); ok {
		cfg.URL = url
	}

	return &cfg, nil
}

func (cfg *Config) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	host := req.Host
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	payload := WakeRequest{
		Host: host,
	}

	body, err := json.Marshal(payload)
	if err != nil {
		cfg.serveLandingPage(rw, host)
		return
	}

	ctx, cancel := context.WithTimeout(req.Context(), cfg.Timeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, cfg.URL, strings.NewReader(string(body)))
	if err != nil {
		cfg.serveLandingPage(rw, host)
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: cfg.Timeout}
	resp, err := client.Do(httpReq)
	if err != nil {
		cfg.serveLandingPage(rw, host)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		rw.Header().Set("X-Wake-Status", "ready")
		return
	}

	cfg.serveLandingPage(rw, host)
}

func (cfg *Config) serveLandingPage(rw http.ResponseWriter, host string) {
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