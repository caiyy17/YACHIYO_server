import asyncio
import os
import sys
import json
import tempfile
from fastapi import FastAPI, Request, HTTPException, Form
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates
import requests
from pydantic import BaseModel
from typing import Optional

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = FastAPI(title="YACHIYO Web UI", description="Web UI for client and configuration management")

templates = Jinja2Templates(directory="templates")

SERVER_URL = "http://localhost:8910"
UPSTREAM_TIMEOUT = (5, 30)
INIT_PIPELINE_TIMEOUT = (5, 300)


class TestResult(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None


async def _request_object(request: Request):
    try:
        data = await request.json()
    except ValueError as e:
        raise HTTPException(status_code=400, detail="Invalid JSON request body") from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=400, detail="JSON request body must be an object")
    return data


async def _server_json(method: str, path: str, *, timeout=UPSTREAM_TIMEOUT, **kwargs):
    """Call the main server without blocking FastAPI's event loop."""
    try:
        response = await asyncio.to_thread(
            requests.request,
            method,
            f"{SERVER_URL}{path}",
            timeout=timeout,
            **kwargs,
        )
    except requests.Timeout as e:
        raise HTTPException(status_code=504, detail="Main server request timed out") from e
    except requests.RequestException as e:
        raise HTTPException(
            status_code=502, detail=f"Main server request failed: {e}"
        ) from e

    if not 200 <= response.status_code < 300:
        detail = response.text or f"Main server returned HTTP {response.status_code}"
        raise HTTPException(status_code=response.status_code, detail=detail)

    try:
        data = response.json()
    except ValueError as e:
        raise HTTPException(
            status_code=502, detail="Main server returned invalid JSON"
        ) from e
    if not isinstance(data, dict):
        raise HTTPException(status_code=502, detail="Main server returned invalid JSON")
    return data


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/pipeline-editor", response_class=HTMLResponse)
async def pipeline_editor():
    editor_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "pipeline_editor.html")
    with open(editor_path, "r", encoding="utf-8") as f:
        return HTMLResponse(content=f.read())


@app.get("/favicon.ico")
async def favicon():
    return JSONResponse(content={"message": "Favicon not found"}, status_code=404)


# ── Client Management APIs ──


@app.get("/api/clients")
async def get_clients():
    """Get all registered clients"""
    data = await _server_json("GET", "/clients/")
    return TestResult(success=True, message="Success", data=data)


@app.get("/api/client/{client_id}")
async def get_client(client_id: str):
    """Get single client info"""
    data = await _server_json("GET", f"/clients/{client_id}")
    return TestResult(success=True, message="Success", data=data)


@app.post("/api/register")
async def register_client(request: Request):
    """Register a new client"""
    data = await _request_object(request)
    client_id = data.get("client_id")
    if not client_id:
        raise HTTPException(status_code=400, detail="Client ID is required")
    result = await _server_json(
        "POST", "/register/", json={"client_id": client_id}
    )
    return TestResult(success=True, message="Registered", data=result)


@app.post("/api/unregister")
async def unregister_client(request: Request):
    """Unregister a client"""
    data = await _request_object(request)
    client_id = data.get("client_id")
    if not client_id:
        raise HTTPException(status_code=400, detail="Client ID is required")
    result = await _server_json(
        "POST", "/unregister/", json={"client_id": client_id}
    )
    return TestResult(success=True, message="Unregistered", data=result)


@app.post("/api/init_pipeline/{client_id}")
async def init_pipeline(client_id: str, request: Request):
    """Initialize pipeline for a client"""
    data = await _request_object(request)
    config_name = data.get("config")
    force = data.get("force", False)
    if not config_name:
        raise HTTPException(status_code=400, detail="Config name is required")
    result = await _server_json(
        "POST",
        f"/init_pipeline/{client_id}",
        timeout=INIT_PIPELINE_TIMEOUT,
        json={"config": config_name, "force": force},
    )
    return TestResult(success=True, message="Pipeline initialized", data=result)


@app.get("/api/logs/{client_id}")
async def get_logs(client_id: str):
    """Get client logs"""
    data = await _server_json("GET", f"/logs/{client_id}")
    return TestResult(success=True, message="Logs retrieved", data=data)


# ── Configuration Management APIs ──


@app.get("/api/configs")
async def get_configs():
    """Get available config files"""
    try:
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_files = []

        configs_dir = os.path.join(parent_dir, "configs")
        for file in os.listdir(configs_dir):
            if file.endswith(".json"):
                config_name = file[:-5]
                config_files.append(
                    {"name": config_name, "type": "config", "path": "configs"}
                )

        lorebooks_dir = os.path.join(configs_dir, "lorebooks")
        for file in os.listdir(lorebooks_dir):
            if file.endswith(".json"):
                config_name = file[:-5]
                config_files.append(
                    {
                        "name": config_name,
                        "type": "lorebook",
                        "path": "configs/lorebooks",
                    }
                )

        return TestResult(
            success=True, message="Success", data={"configs": config_files}
        )
    except OSError as e:
        raise HTTPException(status_code=500, detail=f"Failed to list configs: {e}") from e


@app.get("/api/config/{config_type}/{config_name}")
async def get_config_content(config_type: str, config_name: str):
    """Get config file content"""
    try:
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        if config_type == "config":
            config_file = os.path.join(parent_dir, "configs", f"{config_name}.json")
        elif config_type == "lorebook":
            config_file = os.path.join(
                parent_dir, "configs", "lorebooks", f"{config_name}.json"
            )
        else:
            raise HTTPException(status_code=400, detail="Unsupported config type")

        if not os.path.exists(config_file):
            raise HTTPException(
                status_code=404,
                detail=f"Config file not found: {config_name}.json",
            )

        with open(config_file, "r", encoding="utf-8") as f:
            content = f.read()

        is_editable = "tmp" in config_name.lower()

        return TestResult(
            success=True,
            message="Success",
            data={
                "content": content,
                "config_name": config_name,
                "config_type": config_type,
                "is_editable": is_editable,
            },
        )
    except HTTPException:
        raise
    except (OSError, UnicodeError) as e:
        raise HTTPException(status_code=500, detail=f"Failed to read config: {e}") from e


@app.post("/api/config/{config_type}/{config_name}")
async def save_config_content(
    config_type: str, config_name: str, content: str = Form(...)
):
    """Save config file content (tmp configs only)"""
    temp_file = None
    try:
        if "tmp" not in config_name.lower():
            raise HTTPException(
                status_code=400, detail="Only 'tmp' config files are editable"
            )

        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            raise HTTPException(status_code=400, detail=f"JSON format error: {e}") from e

        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        if config_type == "config":
            config_file = os.path.join(parent_dir, "configs", f"{config_name}.json")
        elif config_type == "lorebook":
            config_file = os.path.join(
                parent_dir, "configs", "lorebooks", f"{config_name}.json"
            )
        else:
            raise HTTPException(status_code=400, detail="Unsupported config type")

        if os.path.exists(config_file):
            backup_file = f"{config_file}.backup"
            with open(config_file, "r", encoding="utf-8") as f:
                backup_content = f.read()
            with open(backup_file, "w", encoding="utf-8") as f:
                f.write(backup_content)

        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=os.path.dirname(config_file),
            delete=False,
        ) as f:
            temp_file = f.name
            f.write(content)
        os.replace(temp_file, config_file)

        return TestResult(
            success=True,
            message="Saved",
            data={"config_name": config_name, "config_type": config_type},
        )
    except HTTPException:
        raise
    except (OSError, UnicodeError) as e:
        raise HTTPException(status_code=500, detail=f"Save failed: {e}") from e
    finally:
        if temp_file:
            try:
                os.unlink(temp_file)
            except FileNotFoundError:
                pass
