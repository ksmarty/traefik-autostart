import asyncio
import json
import os
import threading
import time
from datetime import datetime, timedelta
from typing import Optional
from contextlib import asynccontextmanager

import docker
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse


TRAEFIK_API_URL = os.getenv("TRAEFIK_API_URL", "http://traefik:8080")
DOCKER_SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "templates")

print(f"Environment: TRAEFIK_API_URL={TRAEFIK_API_URL}, DOCKER_SOCKET={DOCKER_SOCKET}")
print(f"Docker socket path: {DOCKER_SOCKET}")

containers_state: dict[str, dict] = {}
last_request_time: dict[str, datetime] = {}
event_queue: asyncio.Queue = asyncio.Queue()

client: Optional[docker.DockerClient] = None


def get_docker_client() -> docker.DockerClient:
    global client
    if client is None:
        socket_path = os.environ.get("DOCKER_SOCKET", "").strip() or "/var/run/docker.sock"
        print(f"Connecting to Docker socket: {socket_path}")
        try:
            client = docker.DockerClient(base_url=f"unix://{socket_path}")
            client.ping()
            print("Docker connection successful")
        except Exception as e:
            print(f"Docker connection failed: {e}")
            raise
    return client


class ContainerManager:
    def __init__(self):
        self.docker = get_docker_client()
        self.lock = threading.Lock()

    def find_container_by_host(self, host: str) -> Optional[dict]:
        try:
            resp = httpx.get(f"{TRAEFIK_API_URL}/api/rawdata", timeout=10)
            resp.raise_for_status()
            data = resp.json()

            routers = data.get("routers", {})

            for router_name, router_data in routers.items():
                rule = router_data.get("rule", "")
                if f"Host(`{host}`)" in rule or f"Host:{host}" in rule:
                    service_name = router_data.get("service")
                    if not service_name:
                        continue

                    services = data.get("services", {})
                    service_data = services.get(service_name, {})

                    provider = service_data.get("provider", "")
                    if "@docker" in provider or "docker" in provider.lower():
                        server_url = service_data.get("serverURL", "")
                        if "://" in server_url:
                            container_name = server_url.split("://")[1].split(":")[0].replace("/", "")
                            return self.find_container_by_name(container_name)

            return None
        except Exception as e:
            print(f"Error querying Traefik API: {e}")
            return None

    def find_container_by_name(self, name: str) -> Optional[dict]:
        try:
            container = self.docker.containers.get(name)
            return {
                "id": container.id,
                "name": container.name,
                "status": container.status,
                "labels": container.labels,
                "short_id": container.short_id,
            }
        except docker.errors.NotFound:
            return None

    def find_managed_containers(self) -> list[dict]:
        containers = []
        try:
            all_containers = self.docker.containers.list(all=True)
            for c in all_containers:
                if c.labels.get("traefik.docker.allownonrunning") == "true":
                    containers.append({
                        "id": c.id,
                        "name": c.name,
                        "status": c.status,
                        "labels": c.labels,
                        "short_id": c.short_id,
                        "service_name": c.labels.get("com.docker.compose.service", c.name),
                        "project": c.labels.get("com.docker.compose.project", ""),
                    })
        except Exception as e:
            print(f"Error listing containers: {e}")
        return containers

    def start_container(self, container_name: str) -> bool:
        try:
            container = self.docker.containers.get(container_name)
            if container.status != "running":
                container.start()
                self._wait_for_health(container)
            return True
        except Exception as e:
            print(f"Error starting container: {e}")
            return False

    def stop_container(self, container_name: str) -> bool:
        try:
            container = self.docker.containers.get(container_name)
            if container.status == "running":
                container.stop()
            return True
        except Exception as e:
            print(f"Error stopping container: {e}")
            return False

    def _wait_for_health(self, container, timeout: int = 60):
        start = time.time()
        while time.time() - start < timeout:
            container.reload()
            health = container.attrs.get("State", {}).get("Health", {})
            if not health:
                if container.status == "running":
                    return
            else:
                status = health.get("Status")
                if status == "healthy":
                    return
            time.sleep(1)

    def get_container_status(self, container_name: str) -> str:
        try:
            container = self.docker.containers.get(container_name)
            health = container.attrs.get("State", {}).get("Health", {})
            if health:
                return health.get("Status", "unknown")
            return container.status
        except:
            return "unknown"


manager = ContainerManager()


async def check_inactivity():
    while True:
        try:
            managed = manager.find_managed_containers()
            now = datetime.now()

            for container in managed:
                name = container["name"]
                labels = container["labels"]

                stop_delay_str = labels.get("autostart.stop-delay", "10m")
                try:
                    stop_delay = timedelta(seconds=parse_duration(stop_delay_str))
                except:
                    stop_delay = timedelta(minutes=10)

                last_time = last_request_time.get(name)
                if last_time and (now - last_time) >= stop_delay:
                    if container["status"] == "running":
                        success = manager.stop_container(name)
                        await event_queue.put({
                            "type": "auto_stop",
                            "container": name,
                            "message": f"Stopped due to inactivity ({stop_delay_str} timeout)",
                        })
                        print(f"Auto-stopped {name} due to inactivity")

            await asyncio.sleep(30)
        except Exception as e:
            print(f"Error in inactivity check: {e}")
            await asyncio.sleep(30)


def parse_duration(s: str) -> float:
    s = s.strip().lower()
    if s.endswith("s"):
        return float(s[:-1])
    elif s.endswith("m"):
        return float(s[:-1]) * 60
    elif s.endswith("h"):
        return float(s[:-1]) * 3600
    return float(s)


async def event_generator():
    while True:
        event = await event_queue.get()
        yield json.dumps(event)


def stop_all_managed_containers():
    try:
        managed = manager.find_managed_containers()
        stopped = []
        for container in managed:
            if container["status"] == "running":
                name = container["name"]
                success = manager.stop_container(name)
                if success:
                    stopped.append(name)
                    print(f"Stopped container on startup: {name}")
        if stopped:
            print(f"Startup: Stopped {len(stopped)} container(s): {', '.join(stopped)}")
        else:
            print("Startup: No managed containers were running")
    except Exception as e:
        print(f"Error stopping containers on startup: {e}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    stop_all_managed_containers()
    asyncio.create_task(check_inactivity())
    yield


app = FastAPI(title="Container Sleep Controller", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.post("/wake")
async def wake(request: Request):
    try:
        body = await request.json()
        host = body.get("host")
    except:
        raise HTTPException(status_code=400, detail="Invalid JSON")

    if not host:
        raise HTTPException(status_code=400, detail="Missing host")

    container = manager.find_container_by_host(host)
    if not container:
        raise HTTPException(status_code=404, detail=f"No container found for host: {host}")

    name = container["name"]
    labels = container["labels"]

    last_request_time[name] = datetime.now()

    if container["status"] != "running":
        success = manager.start_container(name)
        if success:
            await event_queue.put({
                "type": "wake",
                "container": name,
                "message": f"Container started for host: {host}",
            })
        else:
            raise HTTPException(status_code=500, detail="Failed to start container")
    else:
        await event_queue.put({
            "type": "request",
            "container": name,
            "message": f"Request forwarded for host: {host}",
        })

    containers_state[name] = {
        "status": manager.get_container_status(name),
        "last_request": datetime.now().isoformat(),
        "labels": labels,
    }

    return {"status": "ok", "container": name}


@app.get("/containers")
async def list_containers():
    containers = manager.find_managed_containers()
    result = []

    for c in containers:
        name = c["name"]
        state = containers_state.get(name, {})

        stop_delay = c["labels"].get("autostart.stop-delay", "not set")
        last_req = last_request_time.get(name)

        result.append({
            "name": name,
            "short_id": c["short_id"],
            "status": c["status"],
            "health": manager.get_container_status(name),
            "stop_delay": stop_delay,
            "last_request": last_req.isoformat() if last_req else None,
            "service": c["service_name"],
            "project": c["project"],
            "labels": c["labels"],
        })

    return result


@app.post("/containers/{name}/start")
async def start_container(name: str):
    success = manager.start_container(name)
    if success:
        await event_queue.put({
            "type": "manual_start",
            "container": name,
            "message": f"Container manually started",
        })
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Failed to start container")


@app.post("/containers/{name}/stop")
async def stop_container(name: str):
    success = manager.stop_container(name)
    if success:
        await event_queue.put({
            "type": "manual_stop",
            "container": name,
            "message": f"Container manually stopped",
        })
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Failed to stop container")


@app.get("/events")
async def events():
    return EventSourceResponse(event_generator())


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
async def index():
    with open(os.path.join(STATIC_DIR, "index.html"), "r") as f:
        return f.read()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=5000)