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

	// Fast path: container is already running — forward immediately.
	if a.callWake(req.Context(), host) {
		rw.Header().Set("X-Wake-Status", "ready")
		a.next.ServeHTTP(rw, req)
		return
	}

	// Slow path: stream the loading page over the open HTTP connection.
	//
	// Instead of serving a static page with JS polling (which causes the browser to
	// briefly show a blank page on each reload), we keep this HTTP response open and
	// push <script> tags into the live page as status changes. The CSS animation never
	// resets because the page is never reloaded — only the final window.location.reload()
	// triggers a navigation once the container is confirmed ready.
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Header().Set("X-Accel-Buffering", "no") // disable nginx/proxy buffering
	rw.WriteHeader(http.StatusServiceUnavailable)

	flusher, canFlush := rw.(http.Flusher)

	// Write the opening HTML — body is intentionally left unclosed so we can
	// append <script> chunks to the live document stream below.
	fmt.Fprint(rw, strings.ReplaceAll(landingPageHTML, "{{HOST}}", host))
	if canFlush {
		flusher.Flush()
	}

	start := time.Now()
	deadline := time.NewTimer(a.timeout)
	defer deadline.Stop()
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-req.Context().Done():
			// Browser navigated away — stop streaming.
			return

		case <-deadline.C:
			push(rw, flusher, canFlush, fmt.Sprintf(
				`u('Container is taking longer than expected\u2026',%d)`,
				int(a.timeout.Seconds()),
			))
			return

		case <-tick.C:
			elapsed := int(time.Since(start).Seconds())

			if a.callWake(req.Context(), host) {
				// Container is ready — trigger a clean browser navigation to the real page.
				push(rw, flusher, canFlush, "window.location.reload()")
				return
			}

			// Inject a status update reflecting the current startup phase.
			var msg string
			switch {
			case elapsed < 5:
				msg = `Starting container\u2026`
			case elapsed < 20:
				msg = `Waiting for health check\u2026`
			case elapsed < 40:
				msg = `Registering with Traefik\u2026`
			default:
				msg = `Almost ready\u2026`
			}
			push(rw, flusher, canFlush, fmt.Sprintf("u('%s',%d)", msg, elapsed))
		}
	}
}

// callWake POSTs to the controller wake endpoint. Returns true only when the
// controller responds 200 (container is running and Traefik backend is registered).
func (a *AutoStart) callWake(ctx context.Context, host string) bool {
	body, err := json.Marshal(WakeRequest{Host: host})
	if err != nil {
		return false
	}

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.url, strings.NewReader(string(body)))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}

// push injects a JavaScript expression into the open streaming HTML response.
func push(rw http.ResponseWriter, flusher http.Flusher, canFlush bool, js string) {
	fmt.Fprintf(rw, "<script>%s</script>", js)
	if canFlush {
		flusher.Flush()
	}
}

// WakeRequest is the payload sent to the controller's /wake endpoint.
type WakeRequest struct {
	Host string `json:"host"`
}

// landingPageHTML is the initial chunk streamed to the browser. The document is
// intentionally left unclosed — subsequent push() calls extend the live page with
// <script> tags that update status text or trigger a reload when ready.
const landingPageHTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Starting {{HOST}}</title>
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
      min-width: 280px;
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
    h1   { font-size: 1.1rem; font-weight: 600; color: #111827; margin-bottom: 0.75rem; }
    #msg { font-size: 0.9rem; color: #6b7280; min-height: 1.4em; }
    #sec { font-size: 0.75rem; color: #9ca3af; margin-top: 0.4rem; min-height: 1em; }
  </style>
</head>
<body>
  <div class="card">
    <div class="spinner"></div>
    <h1>{{HOST}}</h1>
    <p id="msg">Starting&hellip;</p>
    <p id="sec"></p>
  </div>
  <script>
    function u(msg, secs) {
      document.getElementById('msg').textContent = msg;
      document.getElementById('sec').textContent = secs + 's elapsed';
    }
  </script>`
