import asyncio
import uvicorn
from concurrent.futures.process import ProcessPoolExecutor
from http import HTTPStatus
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi import BackgroundTasks
from typing import Dict,Optional
from uuid import UUID, uuid4
from fastapi import FastAPI
from pydantic import BaseModel, Field
import docker
import os
from fastapi.staticfiles import StaticFiles
import urllib.parse
class MediaObject(BaseModel):
    id: str
    url: str
    meta_data: Optional[dict] = None

class Job(BaseModel):
    uid: UUID = Field(default_factory=uuid4)
    status: str = "in_progress"
    result: int = None

app = FastAPI()
app.mount("/videos", StaticFiles(directory="videos"), name="videos")
jobs: Dict[UUID, Job] = {}
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
def cpu_bound_func(item: MediaObject):
    client = docker.from_env()
    cwd = os.getcwd()
    os.mkdir(f"{cwd}/videos/{item.id}")



    container_id = client.containers.run("jrottenberg/ffmpeg",

    f"""-re -i {item.url} -map 0 -map 0 -c:a libfdk_aac -c:v libx264 \
        -b:v:0 800k -b:v:1 100k -s:v:1 160x80 -profile:v:1 baseline \
        -profile:v:0 main -bf 1 -keyint_min 120 -g 120 -sc_threshold 0 \
        -b_strategy 0 -ar:a:1 22050 -use_timeline 1 -use_template 1 \
        -window_size 5 -adaptation_sets "id=0,streams=v id=1,streams=a" \
        -f dash /mnt/{item.id}.mpd""" ,
                               volumes=[f"{cwd}/videos/{item.id}:/mnt"] ,detach=True)

    container = client.containers.get(container_id.id)
    for line in container.logs(stream=True):
        print(line.strip())

    return (f"http://localhost:8000/videos/{item.id}/{item.id}.mpd")

async def run_in_process(fn, *args):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(app.state.executor, fn, *args)  # wait and return result

async def start_cpu_bound_task(uid: UUID, item: MediaObject) -> None:
    jobs[uid].result = await run_in_process(cpu_bound_func, item)
    jobs[uid].status = "complete"

@app.post("/transcode", status_code=HTTPStatus.ACCEPTED)
async def task_handler(item: MediaObject, background_tasks: BackgroundTasks):
    new_task = Job()
    jobs[new_task.uid] = new_task
    background_tasks.add_task(start_cpu_bound_task, new_task.uid, item)
    return new_task

@app.get("/status/{uid}")
async def status_handler(uid: UUID):
    return jobs[uid]

@app.on_event("startup")
async def startup_event():
    app.state.executor = ProcessPoolExecutor()

@app.on_event("shutdown")
async def on_shutdown():
    app.state.executor.shutdown()


if __name__ == "__main__":
    uvicorn.run("main:app",
                host="0.0.0.0",
                port=80,
                reload = True,
                debug='true')