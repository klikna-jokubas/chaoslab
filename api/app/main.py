
from fastapi import FastAPI

app = FastAPI(title="ChaosLab API")


@app.get("/")
def root():
    return {"message": "ChaosLab API is running"}


@app.get("/health")
def health():
    return {"status": "healthy"}
