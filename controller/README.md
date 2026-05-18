# Container Sleep Controller

A FastAPI-based Python controller that manages container lifecycle based on Traefik routing.

## Features

- **Host-to-Container Mapping**: Queries Traefik's API to find containers by hostname
- **Auto-Start**: Starts stopped containers on incoming requests
- **Auto-Stop**: Stops containers after configurable inactivity period
- **Health Check Wait**: Waits for container health before returning 200 OK
- **Docker Compose Support**: Handles containers from Docker Compose projects

## Requirements

```
fastapi==0.109.0
uvicorn==0.27.0
docker==7.0.0
httpx==0.26.0
python-multipart==0.0.6
sse-starlette==2.0.0
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `TRAEFIK_API_URL` | `http://traefik:8080` | Traefik API endpoint |
| `DOCKER_SOCKET` | `/var/run/docker.sock` | Docker socket path |
| `PORT` | `5000` | Controller listen port |

## API Endpoints

### POST /wake

Wake a container by hostname.

**Request:**
```json
{
  "host": "myapp.local"
}
```

**Response (200 OK):**
```json
{
  "status": "ok",
  "container": "myapp"
}
```

**Error Responses:**
- `400`: Missing or invalid host
- `404`: No container found for host
- `500`: Failed to start container

### GET /containers

List all managed containers.

**Response:**
```json
[
  {
    "name": "myapp",
    "short_id": "abc123",
    "status": "running",
    "health": "healthy",
    "stop_delay": "5m",
    "last_request": "2024-01-15T10:30:00",
    "service": "myapp",
    "project": "myproject",
    "labels": {...}
  }
]
```

### POST /containers/{name}/start

Manually start a container.

### POST /containers/{name}/stop

Manually stop a container.

### GET /events

Server-Sent Events stream for real-time updates.

**Event Types:**
- `wake` - Container started for request
- `request` - Request forwarded to running container
- `auto_stop` - Container stopped due to inactivity
- `manual_start` - Manual start button pressed
- `manual_stop` - Manual stop button pressed

### GET /health

Health check endpoint.

## Configuration

### Container Labels

The controller manages containers with these labels:

| Label | Required | Description |
|-------|----------|-------------|
| `traefik.docker.allownonrunning` | Yes | Must be `true` |
| `autostart.stop-delay` | No | Inactivity timeout (default: 10m) |

### Auto-Stop Logic

1. The controller checks container activity every 30 seconds
2. If `last_request_time` + `stop_delay` < `now`, container is stopped
3. The stop delay can be set per-container: `autostart.stop-delay=5m`

## Docker Compose Awareness

The controller detects Docker Compose projects by reading:
- `com.docker.compose.service` - Service name
- `com.docker.compose.project` - Project name

These are displayed in the WebUI alongside the container name.

## Container Lookup

The controller finds containers by:

1. **Query Traefik API**: GET `/api/rawdata`
2. **Find Router**: Look for router with matching `Host()` rule
3. **Get Service**: Extract service name from router config
4. **Get Server URL**: Extract container name from service's `serverURL`
5. **Find Container**: Use Docker SDK to locate container

This approach works with:
- Manual Host() rules
- Auto-generated rules from `traefik.enable=true`
- Docker Compose service names

## Health Check Handling

If a container has a health check defined:
1. Container is started
2. Controller waits for health status: `healthy`
3. Timeout after 60 seconds
4. Returns success anyway if no health check exists

## Running

### Local Development

```bash
pip install -r requirements.txt
uvicorn main:app --reload --env-file .env
python main.py
```

### Docker

```bash
docker build -t controller .
docker run -d -v /var/run/docker.sock:/var/run/docker.sock -e TRAEFIK_API_URL=http://traefik:8080 -p 5000:5000 controller
```

## WebUI

The controller serves a web dashboard at `/` with:
- Live container status table
- Real-time event feed
- Manual start/stop buttons
- Container statistics

Built with Tailwind CSS and Alpine.js.

## Troubleshooting

### Container Not Found
- Ensure container has `traefik.docker.allownonrunning=true`
- Verify Traefik has created a router for the host
- Check TRAEFIK_API_URL is correct

### Can't Start Container
- Verify Docker socket is mounted
- Check container exists and name is correct
- Look at Docker daemon logs

### Auto-Stop Not Working
- Check `autostart.stop-delay` label is set
- Verify requests are being tracked (last_request updates)
- Ensure container status is "running" before stop attempt