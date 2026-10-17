
import asyncio
from contextlib import asynccontextmanager, suppress
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI

WORKER_URL = "http://worker:8001/health"
CHECK_INTERVAL = 5

incidents = []
active_incident = None

monitor_state = {
    "worker": "unknown",
    "last_checked_at": None,
}


def timestamp():
    return datetime.now(timezone.utc).isoformat()


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

            # Create an incident only when there isn't
            # already an active one.
            if status != "healthy" and active_incident is None:
                active_incident = {
                    "id": len(incidents) + 1,
                    "service": "worker",
                    "status": "open",
                    "detected_at": timestamp(),
                    "resolved_at": None,
                }
                incidents.append(active_incident)

                print(
                    f"INCIDENT #{active_incident['id']}: "
                    f"worker is {status}"
                )

            # Resolve the active incident after recovery.
            elif status == "healthy" and active_incident is not None:
                active_incident["status"] = "resolved"
                active_incident["resolved_at"] = timestamp()

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
