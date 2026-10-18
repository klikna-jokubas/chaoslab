
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone

from prometheus_client import (
    Counter,
    Gauge,
    Histogram,
    generate_latest,
    CONTENT_TYPE_LATEST,
)

import httpx
from fastapi import FastAPI, Response

WORKER_URL = "http://worker:8001/health"
CHECK_INTERVAL = 5

incidents = []
active_incident = None

monitor_state = {
    "worker": "unknown",
    "last_checked_at": None,
}

# Current worker health: 1 = healthy, 0 = unhealthy
worker_health = Gauge(
    "chaoslab_worker_healthy",
    "Whether the worker is healthy",
)

incidents_created = Counter(
    "chaoslab_incidents_created_total",
    "Total number of incidents created",
)

open_incidents = Gauge(
    "chaoslab_open_incidents",
    "Number of currently open incidents",
)

recovery_attempts = Counter(
    "chaoslab_recovery_attempts_total",
    "Total recovery attempts",
    ["result"],
)

recovery_duration = Histogram(
    "chaoslab_recovery_duration_seconds",
    "Time taken to resolve incidents",
)


def timestamp():
    return datetime.now(timezone.utc).isoformat()

async def attempt_recovery(client: httpx.AsyncClient) -> bool:
    try:
        response = await client.post(
            "http://worker:8001/chaos/recover"
        )
        response.raise_for_status()

        print("Recovery action completed successfully")
        return True

    except httpx.HTTPError as error:
        print(f"Recovery attempt failed: {error}")
        return False


async def monitor_worker():
    global active_incident

    async with httpx.AsyncClient(timeout=3.0) as client:
        await asyncio.sleep(2)

        while True:
            try:
                response = await client.get(WORKER_URL)

                if response.status_code == 200:
                    health = response.json()
                    status = (
                        "healthy"
                        if health.get("status") == "healthy"
                        else "unhealthy"
                    )
                else:
                    status = "unhealthy"

            except httpx.HTTPError:
                status = "unreachable"

            monitor_state["worker"] = status
            monitor_state["last_checked_at"] = timestamp()

            worker_health.set(1 if status == "healthy" else 0)

            # Create an incident only when there isn't
            # already an active one.
            if status != "healthy" and active_incident is None:
                active_incident = {
                    "id": len(incidents) + 1,
                    "service": "worker",
                    "status": "open",
                    "detected_at": timestamp(),
                    "resolved_at": None,
                    "recovery_attempts": 0,
                    "recovery_result": None,
                }
                incidents.append(active_incident)

                incidents_created.inc()
                open_incidents.inc()

                print(
                    f"INCIDENT #{active_incident['id']}: "
                    f"worker is {status}"
                )

                print("Attempting automatic recovery...")

                active_incident["recovery_attempts"] += 1

                recovery_succeeded = await attempt_recovery(client)

                active_incident["recovery_result"] = (
                    "success" if recovery_succeeded else "failed"
                )

                result = "success" if recovery_succeeded else "failed"
                recovery_attempts.labels(result=result).inc()

            # Resolve the active incident after recovery.
            elif status == "healthy" and active_incident is not None:
                active_incident["status"] = "resolved"
                active_incident["resolved_at"] = timestamp()

                detected_at = datetime.fromisoformat(
                    active_incident["detected_at"]
                )
                resolved_at = datetime.fromisoformat(
                    active_incident["resolved_at"]
                )

                duration = (resolved_at - detected_at).total_seconds()
                recovery_duration.observe(duration)

                open_incidents.dec()

                print(
                    f"INCIDENT #{active_incident['id']}: "
                    "worker recovered"
                )

                active_incident = None

            await asyncio.sleep(CHECK_INTERVAL)


@asynccontextmanager
async def lifespan(app: FastAPI):
    task = asyncio.create_task(monitor_worker())

    yield

    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


app = FastAPI(
    title="ChaosLab API",
    lifespan=lifespan,
)


@app.get("/")
def root():
    return {"message": "ChaosLab API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}


@app.get("/monitor/status")
def get_monitor_status():
    return monitor_state


@app.get("/incidents")
def get_incidents():
    return incidents


@app.post("/recovery/worker")
async def recover_worker():
    async with httpx.AsyncClient(timeout=3.0) as client:
        try:
            response = await client.post(
                "http://worker:8001/chaos/recover"
            )
            response.raise_for_status()
            return {
                "message": "Worker recovery requested",
                "worker_response": response.json(),
            }
        except httpx.HTTPError as error:
            return {
                "message": "Worker recovery failed",
                "error": str(error),
            }


@app.get("/metrics")
def metrics():
    return Response(
        content=generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )