# traefik-container-sleep

A "Wake-on-Request" system for Traefik v3 that automatically starts Docker containers on incoming HTTP requests and stops them after a configurable period of inactivity.

GitHub: https://github.com/ksmarty/traefik-container-sleep

## Installation

### Controller (Docker)

Pull the controller image from GitHub Container Registry:

```bash
docker pull ghcr.io/ksmarty/traefik-container-sleep:latest
```

Or build locally:

```bash
docker build -t ghcr.io/ksmarty/traefik-container-sleep:latest ./controller
```

### Traefik Plugin

The plugin can be loaded from GitHub releases or locally.

#### From GitHub (Recommended)

Add this to your Traefik static config:

```yaml
experimental:
  plugins:
    github.com/ksmarty/traefik-container-sleep:
      version: v1.0.0
      url: https://github.com/ksmarty/traefik-container-sleep/releases/download/v1.0.0/plugin.tar.gz
```

Then reference it in your dynamic config:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        github.com/ksmarty/traefik-container-sleep:
          timeout: 30
          url: http://controller:5000/wake
```

#### Locally (Development)

Mount the plugin directory and enable local plugins:

```yaml
services:
  traefik:
    command:
      - "--experimental.localPlugins=true"
    volumes:
      - ./traefik-plugin:/plugins-local
    environment:
      - TRAEFIK_PLUGIN_LOCAL=/plugins-local
```

Then configure in your dynamic config:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

## Quick Start

### Minimal Docker Compose

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
      - ./traefik-config:/etc/traefik/dynamic

  controller:
    image: ghcr.io/ksmarty/traefik-container-sleep:latest
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - TRAEFIK_API_URL=http://traefik:8080
```

### Full Example

```bash
git clone https://github.com/ksmarty/traefik-container-sleep.git
cd traefik-container-sleep
docker compose up -d
```

### Access Points

| Service | URL | Description |
|---------|-----|-------------|
| WebUI | http://localhost:5000 | Controller dashboard |
| Traefik Dashboard | http://localhost:8080/dashboard/ | Traefik admin |
| Test Endpoint | http://localhost:80 | Use Host: sleepy.local |

## Architecture

```
┌─────────────┐    HTTP Request     ┌──────────────┐
│   Client    │ ──────────────────► │   Traefik    │
└─────────────┘                     └──────┬───────┘
                                            │
                                     ┌──────▼───────┐
                                     │   Middleware │
                                     │  (autostart) │
                                     └──────┬───────┘
                                            │ POST /wake
                                     ┌──────▼───────┐
                                     │  Controller  │
                                     │  (FastAPI)   │
                                     └──────┬───────┘
                                            │
                                     ┌──────▼───────┐
                                     │    Docker    │
                                     │   Engine     │
                                     └──────────────┘
```

## Configuration

### Container Labels

Add these labels to your containers to enable auto-start:

```yaml
labels:
  - "traefik.enable=true"
  - "traefik.docker.allownonrunning=true"
  - "traefik.http.routers.myservice.rule=Host(`myapp.local`)"
  - "traefik.http.routers.myservice.entrypoints=web"
  - "traefik.http.routers.myservice.middlewares=autostart@file"
  - "traefik.http.services.myservice.loadbalancer.server.port=80"
  - "autostart.stop-delay=5m"
```

The label `traefik.docker.allownonrunning=true` keeps the router active while the container is stopped. The middleware `autostart@file` triggers the wake-on-request behavior. The `autostart.stop-delay` label configures how long to wait before stopping an idle container.

### Controller Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TRAEFIK_API_URL` | `http://traefik:8080` | Traefik API endpoint |
| `DOCKER_SOCKET` | `/var/run/docker.sock` | Docker socket path |
| `PORT` | `5000` | Controller listen port |

### Plugin Options

| Option | Default | Description |
|--------|---------|-------------|
| `timeout` | `30` | Seconds to wait for container to start |
| `url` | `http://controller:5000/wake` | Controller wake endpoint |

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
Check container has `traefik.docker.allownonrunning=true` label. Verify middleware is applied: `traefik.http.routers.<name>.middlewares=autostart@file`. Check controller logs: `docker compose logs controller`.

### Middleware Not Found
Ensure the dynamic config is mounted: `- ./traefik-config:/etc/traefik/dynamic`

### Docker Socket Access
Ensure the controller has access to `/var/run/docker.sock`:

```yaml
volumes:
  - /var/run/docker.sock:/var/run/docker.sock
```

## Project Structure

```
.
├── docker-compose.yml
├── README.md
├── traefik-plugin/
│   ├── README.md
│   ├── middleware.go
│   ├── .traefik.yml
│   └── go.mod
├── traefik-config/
│   └── dynamic.yaml
└── controller/
    ├── README.md
    ├── main.py
    ├── requirements.txt
    ├── Dockerfile
    └── templates/
        └── index.html
```

## License

MIT