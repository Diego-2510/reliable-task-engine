import uuid
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict

from task_engine.db import Session
from task_engine.models import Job

app = FastAPI()


class JobInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payload: dict[str, Any]


class JobOutput(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    payload: dict[str, Any]
    status: str
    attempts: int
    created_at: datetime
    updated_at: datetime


@app.exception_handler(RequestValidationError)
async def invalid_input(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"error": {"code": "invalid_input", "message": "Invalid request"}})


@app.post("/v1/jobs", status_code=201, response_model=JobOutput)
def create_job(body: JobInput) -> Job:
    with Session.begin() as session:
        job = Job(payload=body.payload)
        session.add(job)
        session.flush()
    return job


@app.get("/v1/jobs/{job_id}", response_model=JobOutput, responses={404: {"description": "Job not found"}})
def get_job(job_id: uuid.UUID) -> Any:
    with Session() as session:
        job = session.get(Job, job_id)
        if job is None:
            return JSONResponse(status_code=404, content={"error": {"code": "not_found", "message": "Job not found"}})
        return job
