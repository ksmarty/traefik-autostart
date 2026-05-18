# Wake-on-Request: Auto-Start/Stop Containers with Traefik

A "Wake-on-Request" system for Traefik v3 that automatically starts Docker containers on incoming HTTP requests and stops them after a configurable period of inactivity.

GitHub: https://github.com/ksmarty/traefik-container-sleep

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

## Components

### 1. Traefik Middleware Plugin
Extracts the Host header from incoming requests, sends a POST request to the controller, holds the request until the container is ready, and serves a "Service Starting..." landing page during startup.

### 2. Python Controller
FastAPI-based REST API that queries Traefik's `/api/rawdata` to map Hosts to containers, starts/stops Docker containers via Docker SDK, and monitors container activity for auto-stop.

### 3. Interactive WebUI
Real-time dashboard showing all managed containers with manual start/stop controls and a live event feed.

## Quick Start

### Prerequisites
Docker and Docker Compose with Traefik v3.1+

### Run the System

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

### Controller Configuration

The controller accepts these environment variables:

```yaml
services:
  controller:
    environment:
      - TRAEFIK_API_URL=http://traefik:8080
      - DOCKER_SOCKET=/var/run/docker.sock
      - PORT=5000
```

The `TRAEFIK_API_URL` should point to Traefik's API endpoint. The `DOCKER_SOCKET` must be mounted to access the Docker daemon.

### Adding the Plugin

Mount the plugin directory and enable local plugins in Traefik:

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

Then configure the middleware in your dynamic config:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

The timeout option controls how long the middleware waits for the container to start. The url should point to your controller's wake endpoint.

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