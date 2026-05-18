import asyncio
import json
import os
import threading
import time
from datetime import datetime, timedelta, timezone
from typing import Optional
from contextlib import asynccontextmanager

import docker
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from sse_starlette.sse import EventSourceResponse


TRAEFIK_API_URL = os.getenv("TRAEFIK_API_URL", "http://traefik:8080")
DOCKER_SOCKET = os.getenv("DOCKER_SOCKET", "/var/run/docker.sock")
STATIC_DIR = os.path.join(os.path.dirname(__file__), "templates")

print(f"Environment: TRAEFIK_API_URL={TRAEFIK_API_URL}, DOCKER_SOCKET={DOCKER_SOCKET}")
print(f"Docker socket path: {DOCKER_SOCKET}")

containers_state: dict[str, dict] = {}
last_request_time: dict[str, datetime] = {}
starting_containers: set[str] = set()
container_start_times: dict[str, datetime] = {}

# Pub-sub: each SSE connection gets its own queue; broadcast() fans out to all of them.
# Using a single shared asyncio.Queue meant multiple connections competing for the same
# events — each event only reached one client instead of all of them.
subscribers: list[asyncio.Queue] = []
events_history: list[dict] = []  # rolling buffer of recent events for new page loads


async def broadcast(event: dict) -> None:
    """Push an event to every connected SSE client and append it to the history buffer.

    'sync' events are operational (periodic container-list pushes) and are never stored
    in events_history — they are not log-worthy and would flood the 100-item buffer.
    """
    event.setdefault("timestamp", datetime.now(timezone.utc).isoformat())
    if event.get("type") != "sync":
        events_history.append(event)
        if len(events_history) > 100:
            events_history.pop(0)
    for q in subscribers:
        await q.put(event)


# ─── Container payload helpers ────────────────────────────────────────────────

def _fetch_container_data(name: str, override_status: str | None = None) -> dict | None:
    """Blocking. Inspect a single managed container and return the webui dict.

    Uses one Docker API call (GET /containers/{id}/json) to obtain both status
    and health in a single round-trip. Returns None if the container is not found.

    If override_status is given it takes priority. Otherwise, containers that are
    in `starting_containers` report "starting" regardless of Docker status — this
    prevents sync events from showing "running" before both Docker health AND
    Traefik registration are confirmed, which would make the webui appear ahead
    of the loading page.
    """
    try:
        c = manager.docker.containers.get(name)
        labels = c.labels
        health_info = c.attrs.get("State", {}).get("Health", {})
        health = health_info.get("Status", c.status) if health_info else c.status
        last_req = last_request_time.get(name)
        if override_status is not None:
            effective_status = override_status
        elif name in starting_containers:
            effective_status = "starting"
        else:
            effective_status = c.status
        return {
            "name": c.name,
            "short_id": c.short_id,
            "status": effective_status,
            "health": health,
            "stop_delay": labels.get("autostart.stop-delay", "not set"),
            "last_request": last_req.isoformat() if last_req else None,
            "service": labels.get("com.docker.compose.service", c.name),
            "project": labels.get("com.docker.compose.project", ""),
            "group": labels.get("autostart.group", ""),
        }
    except Exception:
        return None


def _fetch_all_containers_data() -> list[dict]:
    """Blocking. List all managed containers and inspect each one for its full state.

    Uses one docker list call to enumerate managed containers, then one reload()
    per container to obtain Health status (not available from the list API).
    Total cost: 1 + N Docker API calls where N is the number of managed containers.
    """
    try:
        all_containers = manager.docker.containers.list(all=True)
    except Exception as e:
        print(f"_fetch_all_containers_data list failed: {e}")
        return []
    result = []
    for c in all_containers:
        if not (c.labels.get("autostart.enable") == "true" or c.labels.get("autostart.group")):
            continue
        try:
            c.reload()  # fetch full State including Health
        except Exception:
            pass  # use whatever attrs the list call returned
        labels = c.labels
        health_info = c.attrs.get("State", {}).get("Health", {})
        health = health_info.get("Status", c.status) if health_info else c.status
        last_req = last_request_time.get(c.name)
        # Use "starting" if the container is in our in-flight set — same logic as
        # _fetch_container_data — so periodic sync events never show "running" while
        # Traefik hasn't registered the backend yet.
        status = "starting" if c.name in starting_containers else c.status
        result.append({
            "name": c.name,
            "short_id": c.short_id,
            "status": status,
            "health": health,
            "stop_delay": labels.get("autostart.stop-delay", "not set"),
            "last_request": last_req.isoformat() if last_req else None,
            "service": labels.get("com.docker.compose.service", c.name),
            "project": labels.get("com.docker.compose.project", ""),
            "group": labels.get("autostart.group", ""),
        })
    return result

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
        managed = self.find_managed_containers()

        # Primary: check traefik router rule labels directly on managed containers.
        for c in managed:
            for key, value in c["labels"].items():
                if key.startswith("traefik.http.routers.") and key.endswith(".rule"):
                    if f"Host(`{host}`)" in value:
                        return c

        # Fallback: query Traefik API for containers using defaultRule or other discovery.
        # Docker provider service names are "{slug}@docker"; the slug matches the compose
        # service name (com.docker.compose.service) or an explicit traefik service label.
        # The router name slug (e.g. "pdf" from "pdf@docker") is checked first since Traefik
        # names Docker routers after the container even when a custom service name is used.
        try:
            resp = httpx.get(f"{TRAEFIK_API_URL}/api/rawdata", timeout=10)
            resp.raise_for_status()
            data = resp.json()

            for router_name, router_data in data.get("routers", {}).items():
                rule = router_data.get("rule", "")
                if f"Host(`{host}`)" not in rule:
                    continue

                service_name = router_data.get("service", "")

                service_slug = service_name.rsplit("@", 1)[0] if "@" in service_name else service_name
                router_slug = router_name.rsplit("@", 1)[0] if "@" in router_name else router_name

                for c in managed:
                    labels = c["labels"]
                    if c["name"] in (service_slug, router_slug) or c["service_name"] in (service_slug, router_slug):
                        return c
                    if any(k.startswith(f"traefik.http.services.{service_slug}.") for k in labels):
                        return c

        except Exception as e:
            print(f"Error querying Traefik API: {e}")

        print(f"No container found for host={host}")
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
                # A container is managed if it explicitly opts in OR if it belongs to a group.
                # Having autostart.group on a container is sufficient — autostart.enable is optional.
                if c.labels.get("autostart.enable") == "true" or c.labels.get("autostart.group"):
                    containers.append({
                        "id": c.id,
                        "name": c.name,
                        "status": c.status,
                        "labels": c.labels,
                        "short_id": c.short_id,
                        "service_name": c.labels.get("com.docker.compose.service", c.name),
                        "project": c.labels.get("com.docker.compose.project", ""),
                        "group": c.labels.get("autostart.group", ""),
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
                if status in ("healthy", "unhealthy"):
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


async def wait_for_traefik_backend(host: str, timeout: int = 15) -> bool:
    """Poll Traefik's rawdata API until the service for `host` has at least one UP server.

    This bridges the gap between Docker reporting a container as running/healthy and
    Traefik actually registering the new backend in its load balancer pool.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            resp = httpx.get(f"{TRAEFIK_API_URL}/api/rawdata", timeout=5)
            data = resp.json()
            for router_data in data.get("routers", {}).values():
                if f"Host(`{host}`)" not in router_data.get("rule", ""):
                    continue
                service_name = router_data.get("service", "")
                services = data.get("services", {})
                # Try both "name" and "name@docker" as Traefik may use either form
                for key in (service_name, service_name + "@docker"):
                    servers = services.get(key, {}).get("loadBalancer", {}).get("servers", [])
                    if any(s.get("status") == "UP" for s in servers):
                        return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


async def periodic_container_sync():
    """Push the full managed container list to all SSE clients every 10 s.

    This replaces frontend polling entirely: the webui receives fresh state on a
    fixed cadence without making any HTTP requests. The Docker API calls are skipped
    when no clients are connected.
    """
    while True:
        await asyncio.sleep(10)
        if not subscribers:
            continue
        try:
            containers = await asyncio.to_thread(_fetch_all_containers_data)
            await broadcast({"type": "sync", "containers": containers})
        except Exception as e:
            print(f"periodic_container_sync error: {e}")


async def start_container_task(name: str, group: str, host: str):
    """Start the targeted container and all co-group members concurrently.

    All containers in the group are kicked off at the same time via asyncio.gather
    so there is no sequential delay between members. Each manager.start_container
    call blocks until that container is healthy; the slowest member determines the
    total startup time.

    Only after every concurrent start returns AND Traefik has registered the
    targeted backend is the whole group considered ready. The 'ready' broadcast
    is sent for every member so the webui updates all rows at once.
    """
    # Collect all containers to start: targeted + non-running group members.
    to_start: list[str] = []
    try:
        if group:
            members = await asyncio.to_thread(_get_group_members, group)
            to_start = [c["name"] for c in members]
            if name not in to_start:
                to_start.insert(0, name)
        else:
            to_start = [name]

        # Start all concurrently. manager.start_container is a no-op for already-running
        # containers, so it is safe to include every group member unconditionally.
        results = await asyncio.gather(
            *[asyncio.to_thread(manager.start_container, n) for n in to_start],
            return_exceptions=True,
        )

        # The targeted container must succeed; group member failures are logged but
        # don't block the redirect.
        target_idx = to_start.index(name)
        target_ok = results[target_idx] is True and not isinstance(results[target_idx], Exception)

        if target_ok:
            await wait_for_traefik_backend(host)
            print(f"Group ready: {to_start} for host {host}")
            for n, result in zip(to_start, results):
                state = await asyncio.to_thread(_fetch_container_data, n)
                msg = (f"Container started for host: {host}" if n == name
                       else f"Container started as part of group '{group}'")
                await broadcast({"type": "ready", "container": n, "message": msg, "state": state})
        else:
            print(f"Failed to start targeted container {name}")
            state = await asyncio.to_thread(_fetch_container_data, name)
            await broadcast({
                "type": "error",
                "container": name,
                "message": f"Failed to start container for host: {host}",
                "state": state,
            })
    except Exception as e:
        print(f"start_container_task error: {e}")
        state = await asyncio.to_thread(_fetch_container_data, name)
        await broadcast({
            "type": "error",
            "container": name,
            "message": f"Failed to start container for host: {host}",
            "state": state,
        })
    finally:
        # Remove every group member from starting_containers, not just the targeted one.
        for n in to_start:
            starting_containers.discard(n)
        starting_containers.discard(name)  # safety: handle empty to_start


async def check_inactivity():
    while True:
        try:
            managed = manager.find_managed_containers()
            now = datetime.now(timezone.utc)

            for container in managed:
                name = container["name"]
                labels = container["labels"]
                group = labels.get("autostart.group", "")

                stop_delay_str = labels.get("autostart.stop-delay", "10m")
                try:
                    stop_delay = timedelta(seconds=parse_duration(stop_delay_str))
                except:
                    stop_delay = timedelta(minutes=10)

                last_time = last_request_time.get(name)
                if last_time and (now - last_time) >= stop_delay:
                    if container["status"] == "running":
                        await asyncio.to_thread(manager.stop_container, name)
                        state = await asyncio.to_thread(_fetch_container_data, name)
                        await broadcast({
                            "type": "auto_stop",
                            "container": name,
                            "message": f"Stopped due to inactivity ({stop_delay_str} timeout)",
                            "state": state,
                        })
                        print(f"Auto-stopped {name} due to inactivity")

                        if group:
                            for other in managed:
                                if other["group"] == group and other["name"] != name and other["status"] == "running":
                                    await asyncio.to_thread(manager.stop_container, other["name"])
                                    other_state = await asyncio.to_thread(_fetch_container_data, other["name"])
                                    await broadcast({
                                        "type": "auto_stop",
                                        "container": other["name"],
                                        "message": f"Stopped group {group} due to inactivity",
                                        "state": other_state,
                                    })

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
    q: asyncio.Queue = asyncio.Queue()
    subscribers.append(q)
    try:
        # Immediate ping confirms the connection to the browser.
        yield json.dumps({"type": "ping"})
        while True:
            try:
                event = await asyncio.wait_for(q.get(), timeout=25)
            except asyncio.TimeoutError:
                # Keepalive: prevents proxies from closing idle SSE connections.
                yield json.dumps({"type": "ping"})
                continue
            yield json.dumps(event)
    finally:
        # Clean up when the client disconnects.
        subscribers.remove(q)


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
    # Stop managed containers in the background so the server becomes available immediately.
    # Without this, FastAPI blocks all requests until every container has been stopped.
    asyncio.create_task(asyncio.to_thread(stop_all_managed_containers))
    asyncio.create_task(check_inactivity())
    asyncio.create_task(periodic_container_sync())
    yield


app = FastAPI(title="Traefik Autostart Controller", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def _get_group_members(group: str) -> list[dict]:
    """Blocking. Return all managed containers that belong to the given group."""
    if not group:
        return []
    return [c for c in manager.find_managed_containers()
            if c["labels"].get("autostart.group") == group]


def _build_group_status(members: list[dict]) -> list[dict]:
    """Build the per-container status list embedded in /wake 202 responses."""
    result = []
    for c in members:
        if c["status"] == "running" and c["name"] not in starting_containers:
            st = "ready"
        elif c["name"] in starting_containers:
            st = "starting"
        else:
            st = c["status"]
        result.append({"name": c["name"], "status": st})
    return result


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
    group = labels.get("autostart.group", "")

    now = datetime.now(timezone.utc)
    last_request_time[name] = now

    # ── Fast path: targeted container is running ──────────────────────────────
    if container["status"] == "running" and name not in starting_containers:
        if group:
            members = await asyncio.to_thread(_get_group_members, group)
            all_ready = all(
                c["status"] == "running" and c["name"] not in starting_containers
                for c in members
            )
            if not all_ready:
                # Some group members are still starting — keep the progress page alive.
                start_time = container_start_times.get(name, now)
                elapsed = int((now - start_time).total_seconds())
                return JSONResponse(status_code=202, content={
                    "status": "starting",
                    "container": name,
                    "group_name": group,
                    "elapsed": elapsed,
                    "group": _build_group_status(members),
                })
            # Every member is running — return 200 and include the final group state
            # so the middleware can flash the all-green list before redirecting.
            group_status = [{"name": c["name"], "status": "ready"} for c in members]
            return JSONResponse(content={"status": "ok", "container": name, "group": group_status})

        return JSONResponse(content={"status": "ok", "container": name, "group": []})

    # ── Slow path: start containers ───────────────────────────────────────────
    # Only the first concurrent /wake call for this container fires the task;
    # subsequent calls fall through to the 202 response below.
    group_members_pre: list[dict] = []
    if name not in starting_containers:
        starting_containers.add(name)
        container_start_times[name] = now

        # Pre-add every non-running group member to starting_containers immediately
        # so that sync events and subsequent /wake group-status checks all report
        # "starting" for the whole group from the very first response.
        if group:
            group_members_pre = await asyncio.to_thread(_get_group_members, group)
            for c in group_members_pre:
                if c["name"] != name and c["status"] != "running":
                    starting_containers.add(c["name"])

        asyncio.create_task(start_container_task(name, group, host))

        # Broadcast a "starting" event for every group member that just entered
        # the starting state so the webui immediately reflects the change.
        for c in group_members_pre:
            if c["name"] != name and c["name"] in starting_containers:
                m_state = await asyncio.to_thread(_fetch_container_data, c["name"])
                await broadcast({
                    "type": "starting",
                    "container": c["name"],
                    "message": f"Starting container as part of group '{group}'",
                    "state": m_state,
                })

        state = await asyncio.to_thread(_fetch_container_data, name)
        await broadcast({
            "type": "starting",
            "container": name,
            "message": f"Starting container for host: {host}",
            "state": state,
        })

    # Build group status for the 202 response.
    # Re-use the pre-fetched member list (first request) or fetch it now (concurrent request).
    if group:
        members = group_members_pre or await asyncio.to_thread(_get_group_members, group)
        group_status = _build_group_status(members)
    else:
        group_status = []

    start_time = container_start_times.get(name, now)
    elapsed = int((now - start_time).total_seconds())

    return JSONResponse(status_code=202, content={
        "status": "starting",
        "container": name,
        "group_name": group,
        "elapsed": elapsed,
        "group": group_status,
    })


@app.get("/containers")
async def list_containers():
    return await asyncio.to_thread(_fetch_all_containers_data)


@app.get("/containers/{name}")
async def get_container(name: str):
    """Return the current state of a single managed container."""
    data = await asyncio.to_thread(_fetch_container_data, name)
    if not data:
        raise HTTPException(status_code=404, detail="Container not found")
    return data


@app.post("/containers/{name}/start")
async def start_container(name: str):
    success = await asyncio.to_thread(manager.start_container, name)
    if success:
        last_request_time[name] = datetime.now(timezone.utc)
        state = await asyncio.to_thread(_fetch_container_data, name)
        await broadcast({
            "type": "manual_start",
            "container": name,
            "message": "Container manually started",
            "state": state,
        })
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Failed to start container")


@app.post("/containers/{name}/stop")
async def stop_container(name: str):
    success = await asyncio.to_thread(manager.stop_container, name)
    if success:
        state = await asyncio.to_thread(_fetch_container_data, name)
        await broadcast({
            "type": "manual_stop",
            "container": name,
            "message": "Container manually stopped",
            "state": state,
        })
        return {"status": "ok"}
    raise HTTPException(status_code=500, detail="Failed to stop container")


@app.post("/groups/{name}/stop")
async def stop_group(name: str):
    """Stop every running container that belongs to the named group."""
    containers = manager.find_managed_containers()
    stopped = []
    errors = []
    for c in containers:
        if c["group"] != name:
            continue
        if c["status"] != "running":
            continue
        success = await asyncio.to_thread(manager.stop_container, c["name"])
        if success:
            stopped.append(c["name"])
            state = await asyncio.to_thread(_fetch_container_data, c["name"])
            await broadcast({
                "type": "manual_stop",
                "container": c["name"],
                "message": f"Stopped as part of group '{name}'",
                "state": state,
            })
        else:
            errors.append(c["name"])
    if errors:
        raise HTTPException(status_code=500, detail=f"Failed to stop: {', '.join(errors)}")
    return {"status": "ok", "stopped": stopped}


@app.get("/events/history")
async def get_events_history():
    """Return the most recent events for pre-populating the webui event log."""
    return events_history


@app.get("/events")
async def events():
    return EventSourceResponse(event_generator())


@app.get("/debug/traefik-test")
async def traefik_test():
    """Test connectivity to the Traefik API."""
    try:
        resp = httpx.get(f"{TRAEFIK_API_URL}/api/rawdata", timeout=10)
        resp.raise_for_status()
        data = resp.json()
        routers = list(data.get("routers", {}).keys())
        services = list(data.get("services", {}).keys())
        return {
            "status": "ok",
            "traefik_url": TRAEFIK_API_URL,
            "routers_count": len(routers),
            "services_count": len(services),
            "sample_routers": routers[:5],
            "sample_services": services[:5],
        }
    except Exception as e:
        return {"status": "error", "traefik_url": TRAEFIK_API_URL, "error": str(e)}


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