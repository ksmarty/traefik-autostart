# traefik-container-sleep

A "Wake-on-Request" system for Traefik v3 that automatically starts Docker containers on incoming HTTP requests and stops them after a configurable period of inactivity.

## Installation

### Controller

```bash
docker pull ghcr.io/ksmarty/traefik-container-sleep:latest
```

### Traefik Plugin

#### Using Traefik CLI (Recommended)

```bash
traefik plugin create github.com/ksmarty/traefik-container-sleep
```

Then add to your static config:

```yaml
experimental:
  plugins:
    github.com/ksmarty/traefik-container-sleep:
      version: v1.0.0
```

Or configure the URL manually:

```yaml
experimental:
  plugins:
    github.com/ksmarty/traefik-container-sleep:
      version: v1.0.0
      url: https://github.com/ksmarty/traefik-container-sleep/releases/download/v1.0.0/plugin.tar.gz
```

## Quick Start

```yaml
version: "3.8"

services:
  traefik:
    image: traefik:v3.1
    command:
      - "--api.insecure=true"
      - "--providers.docker=true"
      - "--providers.file.directory=/etc/traefik/dynamic"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./dynamic.yaml:/etc/traefik/dynamic/dynamic.yaml

  controller:
    image: ghcr.io/ksmarty/traefik-container-sleep:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - TRAEFIK_API_URL=http://traefik:8080
```

Create `dynamic.yaml`:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        github.com/ksmarty/traefik-container-sleep:
          timeout: 30
          url: http://controller:5000/wake
```

## Configuration

### Container Labels

Add these labels to your containers to enable auto-start:

```yaml
labels:
  - "autostart.enable=true"
  - "traefik.docker.allownonrunning=true"
  - "traefik.enable=true"
  - "traefik.http.routers.myservice.rule=Host(`myapp.local`)"
  - "traefik.http.routers.myservice.middlewares=autostart"
```

The `autostart.enable=true` label tells the controller to manage this container. The `traefik.docker.allownonrunning=true` label keeps Traefik's router active while the container is stopped.

The `autostart.enable=true` label enables auto-start management. The `autostart.stop-delay` label configures how long to wait before stopping an idle container.

### Optional Labels

| Label | Description |
|-------|-------------|
| `autostart.stop-delay` | Duration before stopping idle container (default: 10m) |
| `autostart.group` | Group name to start/stop containers together |

Example with all options:

```yaml
labels:
  - "autostart.enable=true"
  - "autostart.group=frontend"
  - "autostart.stop-delay=5m"
  - "traefik.enable=true"
  - "traefik.http.routers.app.rule=Host(`app.local`)"
  - "traefik.http.routers.app.middlewares=autostart"
```

### Supported Duration Formats

- `30s` - 30 seconds
- `5m` - 5 minutes
- `1h` - 1 hour

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/wake` | Wake a container by host |
| GET | `/containers` | List all managed containers |
| POST | `/containers/{name}/start` | Manually start a container |
| POST | `/containers/{name}/stop` | Manually stop a container |
| GET | `/events` | SSE event stream |
| GET | `/health` | Health check |

## Troubleshooting

### Container Not Starting
Check container has `autostart.enable=true` label. Verify middleware is applied. Check controller logs.

### Docker Socket Access
Ensure the controller has access to `/var/run/docker.sock`:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

## License

MIT