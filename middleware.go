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
	defaultTimeout = 300 // seconds — generous default to handle slow-starting containers
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

// wakeResult holds the parsed response from the controller's /wake endpoint.
type wakeResult struct {
	ready   bool
	elapsed int
	group   []groupMember
}

// groupMember is a single container in a wake group.
type groupMember struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

// wakeRespBody is the JSON shape of a 202 response from /wake.
type wakeRespBody struct {
	Elapsed int           `json:"elapsed"`
	Group   []groupMember `json:"group"`
}

// WakeRequest is the payload sent to the controller's /wake endpoint.
type WakeRequest struct {
	Host string `json:"host"`
}

func (a *AutoStart) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	host := req.Host
	if idx := strings.Index(host, ":"); idx != -1 {
		host = host[:idx]
	}

	// Fast path: container is already running — forward immediately.
	result := a.callWake(req.Context(), host)
	if result.ready {
		rw.Header().Set("X-Wake-Status", "ready")
		a.next.ServeHTTP(rw, req)
		return
	}

	// Slow path: stream the loading page over the open HTTP connection.
	//
	// The page is sent immediately so the user sees the spinner at once. The middleware
	// then polls the controller every second and pushes <script> chunks into the live
	// document. The CSS animation never resets because the page is never reloaded —
	// window.location.reload() is only injected once the container is confirmed ready.
	//
	// If the timeout is reached (container taking too long), a final script auto-reloads
	// the page after a short delay, which starts a fresh streaming connection.
	rw.Header().Set("Content-Type", "text/html; charset=utf-8")
	rw.Header().Set("X-Accel-Buffering", "no") // disable nginx/proxy buffering
	rw.WriteHeader(http.StatusServiceUnavailable)

	flusher, canFlush := rw.(http.Flusher)

	fmt.Fprint(rw, strings.ReplaceAll(landingPageHTML, "{{HOST}}", host))
	if canFlush {
		flusher.Flush()
	}

	// Use the elapsed value from the server so that a page refresh picks up exactly
	// where the startup left off rather than resetting the phase text to "0s".
	serverElapsed := result.elapsed
	localStart := time.Now()

	// No server-side deadline — the stream stays open until the container is ready or
	// the browser navigates away. The landing page includes a JS fallback that silently
	// polls if the HTTP stream ever closes early (e.g. a proxy timeout).
	tick := time.NewTicker(1 * time.Second)
	defer tick.Stop()

	for {
		select {
		case <-req.Context().Done():
			// Browser navigated away — stop streaming.
			return

		case <-tick.C:
			elapsed := serverElapsed + int(time.Since(localStart).Seconds())

			result = a.callWake(req.Context(), host)
			if result.ready {
				push(rw, flusher, canFlush, "window.location.reload()")
				return
			}

			// Phase-based status message derived from total elapsed time.
			var msg string
			switch {
			case elapsed < 5:
				msg = `Starting container\u2026`
			case elapsed < 20:
				msg = `Waiting for health check\u2026`
			case elapsed < 40:
				msg = `Registering with Traefik\u2026`
			case elapsed < 90:
				msg = `Almost ready\u2026`
			default:
				msg = fmt.Sprintf(`Still starting\u2026 (%ds)`, elapsed)
			}
			push(rw, flusher, canFlush, fmt.Sprintf("u('%s',%d,%s)", msg, elapsed, groupToJS(result.group)))
		}
	}
}

// callWake POSTs to the controller wake endpoint. On 200 it returns ready=true.
// On 202 it parses the body for elapsed seconds and group member statuses so the
// loading page can show accurate phase text and per-container progress.
func (a *AutoStart) callWake(ctx context.Context, host string) wakeResult {
	body, err := json.Marshal(WakeRequest{Host: host})
	if err != nil {
		return wakeResult{}
	}

	callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(callCtx, http.MethodPost, a.url, strings.NewReader(string(body)))
	if err != nil {
		return wakeResult{}
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return wakeResult{}
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusOK {
		return wakeResult{ready: true}
	}

	// Parse 202 body — best-effort; ignore errors.
	var rb wakeRespBody
	json.NewDecoder(resp.Body).Decode(&rb) //nolint
	return wakeResult{elapsed: rb.Elapsed, group: rb.Group}
}

// push injects a JavaScript expression into the open streaming HTML response.
func push(rw http.ResponseWriter, flusher http.Flusher, canFlush bool, js string) {
	fmt.Fprintf(rw, "<script>%s</script>", js)
	if canFlush {
		flusher.Flush()
	}
}

// groupToJS converts a group slice to a JSON array literal safe for inline JS.
func groupToJS(group []groupMember) string {
	if len(group) == 0 {
		return "null"
	}
	b, err := json.Marshal(group)
	if err != nil {
		return "null"
	}
	return string(b)
}

// landingPageHTML is the initial chunk streamed to the browser. The document is
// intentionally left unclosed — push() calls extend the live page with <script>
// chunks that update status text or trigger a reload when ready.
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
      max-width: 420px;
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
    #grp {
      display: none;
      margin-top: 1rem;
      padding-top: 0.75rem;
      border-top: 1px solid #f3f4f6;
    }
    .gi {
      display: flex;
      justify-content: space-between;
      align-items: center;
      padding: 0.2rem 0;
      font-size: 0.8rem;
    }
    .gn { color: #9ca3af; }
    .gr { color: #22c55e; }
    .gw { color: #f59e0b; }
  </style>
</head>
<body>
  <div class="card">
    <div class="spinner"></div>
    <h1>{{HOST}}</h1>
    <p id="msg">Starting&hellip;</p>
    <p id="sec"></p>
    <div id="grp"></div>
  </div>
  <script>
    function u(msg, secs, group) {
      document.getElementById('msg').textContent = msg;
      document.getElementById('sec').textContent = secs + 's elapsed';
      var g = document.getElementById('grp');
      if (group && group.length > 1) {
        g.style.display = 'block';
        g.innerHTML = group.map(function(c) {
          var ready = c.status === 'ready' || c.status === 'running';
          return '<div class="gi"><span class="gn">' + c.name + '</span>' +
            '<span class="' + (ready ? 'gr' : 'gw') + '">' +
            (ready ? '&#10003; ready' : '&#9679; ' + c.status) +
            '</span></div>';
        }).join('');
      } else {
        g.style.display = 'none';
      }
    }
    // Fallback: if the HTTP stream closes early (e.g. a proxy resets the connection),
    // this quietly polls in the background. While the stream is alive each fetch returns
    // 503 and is aborted immediately — no visible effect. When the container is ready
    // (non-503) the page reloads seamlessly.
    (function() {
      var t = setInterval(function() {
        var c = new AbortController();
        fetch(location.href, { credentials: 'include', signal: c.signal })
          .then(function(r) { c.abort(); if (r.status !== 503) { location.reload(); } })
          .catch(function() {});
      }, 3000);
    }());
  </script>`
