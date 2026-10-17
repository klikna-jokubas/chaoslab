
from fastapi import FastAPI, HTTPException

app = FastAPI(title="ChaosLab Worker")

failure_mode = False

@app.get("/")
def root():
    return {"message": "ChaosLab Worker is running"}


@app.get("/health")
def health():
    if failure_mode:
        raise HTTPException(status_code=503, detail="Worker is unhealthy")

    return {"status": "healthy"}


@app.post("/chaos/fail")
def simulate_failure():
    global failure_mode
    failure_mode = True
    return {"message": "Failure mode enabled"}

@app.post("/chaos/recover")
def simulate_recovery():
    global failure_mode
    failure_mode = False
    return {"message": "Failure mode disabled"}