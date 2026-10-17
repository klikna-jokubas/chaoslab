
from fastapi import FastAPI

app = FastAPI(title="ChaosLab Worker")


@app.get("/")
def root():
    return {"message": "ChaosLab Worker is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}
