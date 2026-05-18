# Installation

## 1. Add traefik-plugin topic to your GitHub repo

Go to https://github.com/ksmarty/traefik-container-sleep -> Settings -> General -> Topics and add `traefik-plugin`

## 2. Configure Traefik Static Config

Add to your static configuration (TOML):

```toml
[experimental.plugins.autostart]
moduleName = "github.com/ksmarty/traefik-container-sleep"
version = "v0.0.2"
```

Or YAML:

```yaml
experimental:
  plugins:
    autostart:
      moduleName: github.com/ksmarty/traefik-container-sleep
      version: v0.0.2
```

## 3. Configure Middleware

Add to your dynamic configuration:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

## 4. Add Controller

Add the controller service to your docker-compose:

```yaml
controller:
  image: ghcr.io/ksmarty/traefik-container-sleep:latest
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
  environment:
    - TRAEFIK_API_URL=http://traefik:8080
```

Restart Traefik.