# traefik-container-sleep

Wake-on-request for Docker containers. Keeps services stopped when idle, starts them automatically when traffic arrives, and stops them again after a configurable inactivity timeout.

**How it works:**
1. The Traefik plugin intercepts a request and calls the controller
2. The controller starts the target container and waits until it's healthy
3. The request is forwarded — a loading page is shown in the meantime
4. After `autostart.stop-delay` of inactivity, the controller stops the container

---

## Requirements

- Traefik v3
- Docker with socket access
- The plugin must be loaded as a **local plugin** (it is not yet in the Traefik Plugin Catalog)

---

## 1. Get the plugin files

Download the latest release and extract the plugin files:

```bash
mkdir -p ./plugin
curl -sL https://github.com/ksmarty/traefik-container-sleep/releases/latest/download/plugin.tar.gz \
  | tar -xz -C ./plugin
```

This gives you `./plugin/middleware.go`, `./plugin/.traefik.yml`, and `./plugin/go.mod`.

---

## 2. Configure Traefik

Mount the plugin directory into Traefik and register it as a local plugin.

### Volume mount

```
./plugin:/plugins-local/src/github.com/ksmarty/traefik-container-sleep:ro
```

### Static config

Pick one format:

**CLI flags**
```
--experimental.localPlugins.autostart.modulename=github.com/ksmarty/traefik-container-sleep
```

**YAML** (`traefik.yml`)
```yaml
experimental:
  localPlugins:
    autostart:
      moduleName: github.com/ksmarty/traefik-container-sleep
```

**TOML** (`traefik.toml`)
```toml
[experimental.localPlugins.autostart]
  moduleName = "github.com/ksmarty/traefik-container-sleep"
```

### Dynamic config (middleware definition)

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30                        # seconds to wait for container to become ready
          url: http://controller:5000/wake   # controller address
```

---

## 3. Run the controller

The controller needs access to the Docker socket and the Traefik API.

**docker run**
```bash
docker run -d \
  --name controller \
  --restart unless-stopped \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -e TRAEFIK_API_URL=http://traefik:8080 \
  -p 5000:5000 \
  ghcr.io/ksmarty/traefik-container-sleep:latest
```

**docker compose**
```yaml
controller:
  image: ghcr.io/ksmarty/traefik-container-sleep:latest
  restart: unless-stopped
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
  environment:
    - TRAEFIK_API_URL=http://traefik:8080
  ports:
    - "5000:5000"
```

**Environment variables**

| Variable | Default | Description |
|---|---|---|
| `TRAEFIK_API_URL` | `http://traefik:8080` | Traefik API endpoint |
| `DOCKER_SOCKET` | `/var/run/docker.sock` | Docker socket path |

---

## 4. Label your containers

Add these labels to any container you want managed:

```yaml
labels:
  - "autostart.enable=true"                                  # opt in to management
  - "traefik.enable=true"
  - "traefik.docker.allownonrunning=true"                    # keep Traefik route alive while stopped
  - "traefik.http.routers.myapp.rule=Host(`myapp.example.com`)"
  - "traefik.http.routers.myapp.middlewares=autostart"
  - "traefik.http.services.myapp.loadbalancer.server.port=80"
```

**Optional labels**

| Label | Default | Description |
|---|---|---|
| `autostart.stop-delay` | `10m` | Idle time before the container is stopped (`30s`, `5m`, `1h`) |
| `autostart.group` | — | Stop/start multiple containers together |

---

## Full docker-compose example

```yaml
services:
  traefik:
    image: traefik:v3
    restart: unless-stopped
    command:
      - --api.insecure=true
      - --providers.docker=true
      - --providers.docker.exposedbydefault=false
      - --providers.file.directory=/etc/traefik/dynamic
      - --entrypoints.web.address=:80
      - --experimental.localPlugins.autostart.modulename=github.com/ksmarty/traefik-container-sleep
    ports:
      - "80:80"
      - "8080:8080"
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock:ro
      - ./plugin:/plugins-local/src/github.com/ksmarty/traefik-container-sleep:ro
      - ./dynamic.yml:/etc/traefik/dynamic/dynamic.yml:ro

  controller:
    image: ghcr.io/ksmarty/traefik-container-sleep:latest
    restart: unless-stopped
    volumes:
      - /var/run/docker.sock:/var/run/docker.sock
    environment:
      - TRAEFIK_API_URL=http://traefik:8080
    depends_on:
      - traefik

  myapp:
    image: nginx:alpine
    labels:
      - "autostart.enable=true"
      - "autostart.stop-delay=10m"
      - "traefik.enable=true"
      - "traefik.docker.allownonrunning=true"
      - "traefik.http.routers.myapp.rule=Host(`myapp.example.com`)"
      - "traefik.http.routers.myapp.middlewares=autostart"
      - "traefik.http.services.myapp.loadbalancer.server.port=80"
```

`dynamic.yml`:
```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

---

## Grouping containers

Containers in the same group start and stop together. If any container in the group goes idle, all containers in the group are stopped.

```yaml
labels:
  - "autostart.enable=true"
  - "autostart.group=mystack"
  - "autostart.stop-delay=5m"
```

---

## Controller dashboard

The controller exposes a web dashboard at `http://controller:5000/` showing live container status, last request times, and manual start/stop controls.

---

## Troubleshooting

**Plugin not loading**
- Confirm the `./plugin/` directory contains `middleware.go`, `.traefik.yml`, and `go.mod`
- Confirm the volume mount path is exactly `/plugins-local/src/github.com/ksmarty/traefik-container-sleep`
- Check Traefik logs for plugin errors

**Container not found for host**
- Ensure `autostart.enable=true` is set on the container
- Ensure the container has an explicit `traefik.http.routers.*.rule=Host(...)` label
- Check that `TRAEFIK_API_URL` points to the Traefik API port (default `8080`)
- Verify `traefik.docker.allownonrunning=true` is set so the route exists while stopped

**Container starts but request isn't forwarded**
- The controller returns 200 only after the container is `running` (with health check: `healthy`)
- Increase `timeout` in the middleware config if your container takes longer to start
- Check controller logs: `docker logs controller`
