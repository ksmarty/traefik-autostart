# Installation

## Clone the repository

```bash
git clone https://github.com/ksmarty/traefik-container-sleep.git
cd traefik-container-sleep
```

## Configure Traefik

Add to your static config or command args:

```yaml
experimental:
  localPlugins:
    autostart:
      moduleName: github.com/ksmarty/traefik-container-sleep
```

Mount the plugin directory:

```yaml
volumes:
  - ./traefik-plugin:/plugins-local
environment:
  - TRAEFIK_PLUGIN_LOCAL=/plugins-local
```

Then add the middleware to your dynamic config:

```yaml
http:
  middlewares:
    autostart:
      plugin:
        autostart:
          timeout: 30
          url: http://controller:5000/wake
```

Finally, add the controller service to your docker-compose:

```yaml
controller:
  image: ghcr.io/ksmarty/traefik-container-sleep:latest
  volumes:
    - /var/run/docker.sock:/var/run/docker.sock
  environment:
    - TRAEFIK_API_URL=http://traefik:8080
```

Restart Traefik and the controller.