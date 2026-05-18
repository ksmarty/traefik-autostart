# Traefik Auto-Start Middleware Plugin

A Traefik v3 middleware plugin written in Go (Yaegi-compatible) that intercepts incoming requests and wakes containers on-demand.

## Features

- Extracts the Host header from incoming requests
- Sends a synchronous POST request to the controller
- Holds the request until container is ready (configurable timeout)
- Falls back to a "Service Starting..." HTML landing page on timeout

## Installation

### Option 1: Local Plugin (Development)

1. Mount the plugin directory in Traefik:

```yaml
services:
  traefik:
    image: traefik:v3.1
    volumes:
      - ./traefik-plugin:/plugins-local
    environment:
      - TRAEFIK_PLUGIN_LOCAL=/plugins-local
    command:
      - "--experimental.localPlugins=true"
```

2. Configure the middleware in `dynamic.yaml`:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

3. Apply to your router:

```yaml
labels:
  - "traefik.http.routers.myservice.middlewares=autostart@file"
```

### Option 2: Plugin Registry (Production)

For production, you can package the plugin and load it from Traefik's plugin registry.

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `timeout` | string | `30s` | Maximum time to wait for container to start |
| `url` | string | `http://controller:5000/wake` | Controller endpoint |

### Example Configuration

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 60s
          url: http://controller:5000/wake
```

## How It Works

1. **Request Interception**: The middleware extracts the `Host` header from the incoming HTTP request.

2. **Wake Call**: It sends a POST request to the controller with the following JSON payload:
   ```json
   {
     "host": "example.local"
   }
   ```

3. **Wait for Response**:
   - If controller returns `200 OK`: Request is forwarded to the container
   - If controller returns error or times out: Landing page is served

4. **Landing Page**: Auto-refreshing HTML page that polls until the service is ready.

## Development

### Prerequisites
- Go 1.21+
- Traefik v3.1+

### Plugin Manifest (`.traefik.yml`)

```yaml
middlewares:
  autostart:
    displayName: Auto Start Middleware
    summary: Automatically starts containers on request
    fields:
      timeout:
        type: number
        default: 30
        label: Timeout (seconds)
      url:
        type: string
        default: http://controller:5000/wake
        label: Controller URL
```

### Building

The plugin is compiled by Traefik's Yaegi interpreter at runtime. No separate build step needed.

### Code Structure

```
middleware.go
├── Config struct          # Plugin configuration
├── New()                  # Factory function (required by Yaegi)
├── ServeHTTP()           # Main request handler
├── serveLandingPage()    # Timeout fallback HTML
└── WakeRequest           # JSON payload struct
```

## Yaegi Compatibility

This plugin is designed to run in Traefik's Yaegi interpreter. To ensure compatibility:

- No CGO dependencies
- No direct socket access
- Pure Go standard library
- No external system calls

## Troubleshooting

### Plugin Not Loading
- Verify `TRAEFIK_PLUGIN_LOCAL` environment variable is set
- Check plugin directory is mounted correctly
- Ensure `--experimental.localPlugins=true` is in command

### Timeout Errors
- Increase the `timeout` value in configuration
- Check controller is accessible from Traefik container
- Verify container health check is configured properly

### Landing Page Always Shows
- Check controller logs for errors
- Verify the container exists and has correct labels
- Ensure Docker socket is accessible to controller