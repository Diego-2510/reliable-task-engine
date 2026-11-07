import uuid
from datetime import datetime
from typing import Any

from fastapi import FastAPI, Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

from task_engine.db import Session
from task_engine.models import Job

app = FastAPI()


class JobInput(BaseModel):
    model_config = ConfigDict(extra="forbid")
    payload: dict[str, Any]
    max_attempts: int = Field(default=3, ge=1, le=10, strict=True)


class JobOutput(BaseModel):
    model_config = ConfigDict(from_attributes=True)
    id: uuid.UUID
    payload: dict[str, Any]
    status: str
    max_attempts: int
    available_at: datetime
    last_error_code: str | None
    terminal_reason: str | None
    attempts: int
    created_at: datetime
    updated_at: datetime


@app.exception_handler(RequestValidationError)
async def invalid_input(request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=422, content={"error": {"code": "invalid_input", "message": "Invalid request"}})


@app.exception_handler(SQLAlchemyError)
async def database_error(request: Request, exc: SQLAlchemyError) -> JSONResponse:
    # Driver diagnostics can contain bound keys. Never return or log those details.
    return JSONResponse(status_code=500, content={
        "error": {"code": "database_error", "message": "Database operation failed"},
    })


@app.post("/v1/jobs", status_code=201, response_model=JobOutput, responses={
    200: {"model": JobOutput, "description": "Existing job"},
    409: {"description": "Idempotency conflict"},
})
def create_job(body: JobInput, request: Request, response: Response) -> Any:
    keys = request.headers.getlist("idempotency-key")
    key = keys[0] if keys else None
    if keys and (len(keys) != 1 or not key or len(key) > 128
                 or any(not "!" <= char <= "~" for char in key)):
        return JSONResponse(status_code=422, content={
            "error": {"code": "invalid_input", "message": "Invalid request"},
        })
    with Session.begin() as session:
        # Each statement needs a fresh snapshot after a competing insert commits.
        session.connection(execution_options={"isolation_level": "READ COMMITTED"})
        job = session.scalar(
            insert(Job).values(
                payload=body.payload, max_attempts=body.max_attempts, idempotency_key=key,
            ).on_conflict_do_nothing(
                index_elements=[Job.idempotency_key],
                index_where=Job.idempotency_key.is_not(None),
            ).returning(Job)
        )
        if job is None:
            # Separate statement: the INSERT snapshot may predate the winner's commit.
            job, matches = session.execute(select(
                Job,
                (Job.payload == body.payload) & (Job.max_attempts == body.max_attempts),
            ).where(Job.idempotency_key == key)).one()
            # JSONB equality ignores nested object order and preserves JSON types.
            if not matches:
                return JSONResponse(status_code=409, content={"error": {
                    "code": "idempotency_conflict",
                    "message": "Job parameters differ from the original submission",
                }})
            response.status_code = 200
    return job


@app.get("/v1/jobs/{job_id}", response_model=JobOutput, responses={404: {"description": "Job not found"}})
def get_job(job_id: uuid.UUID) -> Any:
    with Session() as session:
        job = session.get(Job, job_id)
        if job is None:
            return JSONResponse(status_code=404, content={"error": {"code": "not_found", "message": "Job not found"}})
        return job
