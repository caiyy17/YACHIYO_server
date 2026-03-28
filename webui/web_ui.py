import os
import sys
import json
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


class TestResult(BaseModel):
    success: bool
    message: str
    data: Optional[dict] = None


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/favicon.ico")
async def favicon():
    return JSONResponse(content={"message": "Favicon not found"}, status_code=404)


# ── Client Management APIs ──


@app.get("/api/clients")
async def get_clients():
    """Get all registered clients"""
    try:
        response = requests.get(f"{SERVER_URL}/clients/")
        if response.status_code == 200:
            return TestResult(success=True, message="Success", data=response.json())
        else:
            return TestResult(success=False, message=f"Failed: {response.text}")
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


@app.get("/api/client/{client_id}")
async def get_client(client_id: str):
    """Get single client info"""
    try:
        response = requests.get(f"{SERVER_URL}/clients/{client_id}")
        if response.status_code == 200:
            return TestResult(success=True, message="Success", data=response.json())
        else:
            return TestResult(success=False, message=f"Failed: {response.text}")
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


@app.post("/api/register")
async def register_client(request: Request):
    """Register a new client"""
    try:
        data = await request.json()
        client_id = data.get("client_id")
        if not client_id:
            return TestResult(success=False, message="Client ID is required")
        response = requests.post(
            f"{SERVER_URL}/register/", json={"client_id": client_id}
        )
        if response.status_code == 200:
            return TestResult(
                success=True, message="Registered", data=response.json()
            )
        else:
            return TestResult(
                success=False, message=f"Registration failed: {response.text}"
            )
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


@app.post("/api/unregister")
async def unregister_client(request: Request):
    """Unregister a client"""
    try:
        data = await request.json()
        client_id = data.get("client_id")
        if not client_id:
            return TestResult(success=False, message="Client ID is required")
        response = requests.post(
            f"{SERVER_URL}/unregister/", json={"client_id": client_id}
        )
        if response.status_code == 200:
            return TestResult(
                success=True, message="Unregistered", data=response.json()
            )
        else:
            return TestResult(
                success=False, message=f"Unregistration failed: {response.text}"
            )
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


@app.post("/api/init_pipeline/{client_id}")
async def init_pipeline(client_id: str, request: Request):
    """Initialize pipeline for a client"""
    try:
        data = await request.json()
        config_name = data.get("config")
        force = data.get("force", False)
        if not config_name:
            return TestResult(success=False, message="Config name is required")
        response = requests.post(
            f"{SERVER_URL}/init_pipeline/{client_id}",
            json={"config": config_name, "force": force},
        )
        if response.status_code == 200:
            return TestResult(
                success=True, message="Pipeline initialized", data=response.json()
            )
        elif response.status_code == 404:
            return TestResult(
                success=False, message="Client not found. Register first."
            )
        else:
            return TestResult(success=False, message=f"Failed: {response.text}")
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


@app.get("/api/logs/{client_id}")
async def get_logs(client_id: str):
    """Get client logs"""
    try:
        response = requests.get(f"{SERVER_URL}/logs/{client_id}")
        if response.status_code == 200:
            return TestResult(
                success=True, message="Logs retrieved", data=response.json()
            )
        else:
            return TestResult(
                success=False, message=f"Failed: {response.text}"
            )
    except Exception as e:
        return TestResult(success=False, message=f"Request failed: {str(e)}")


# ── Configuration Management APIs ──


@app.get("/api/configs")
async def get_configs():
    """Get available config files"""
    try:
        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_files = []

        configs_dir = os.path.join(parent_dir, "configs")
        if os.path.exists(configs_dir):
            for file in os.listdir(configs_dir):
                if file.endswith(".json"):
                    config_name = file[:-5]
                    config_files.append(
                        {"name": config_name, "type": "config", "path": "configs"}
                    )

        lorebooks_dir = os.path.join(configs_dir, "lorebooks")
        if os.path.exists(lorebooks_dir):
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
    except Exception as e:
        return TestResult(success=False, message=f"Failed: {str(e)}")


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
            return TestResult(success=False, message="Unsupported config type")

        if not os.path.exists(config_file):
            return TestResult(
                success=False, message=f"Config file not found: {config_name}.json"
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
    except Exception as e:
        return TestResult(success=False, message=f"Failed: {str(e)}")


@app.post("/api/config/{config_type}/{config_name}")
async def save_config_content(
    config_type: str, config_name: str, content: str = Form(...)
):
    """Save config file content (tmp configs only)"""
    try:
        if "tmp" not in config_name.lower():
            return TestResult(
                success=False, message="Only 'tmp' config files are editable"
            )

        try:
            json.loads(content)
        except json.JSONDecodeError as e:
            return TestResult(success=False, message=f"JSON format error: {str(e)}")

        parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

        if config_type == "config":
            config_file = os.path.join(parent_dir, "configs", f"{config_name}.json")
        elif config_type == "lorebook":
            config_file = os.path.join(
                parent_dir, "configs", "lorebooks", f"{config_name}.json"
            )
        else:
            return TestResult(success=False, message="Unsupported config type")

        if os.path.exists(config_file):
            backup_file = f"{config_file}.backup"
            with open(config_file, "r", encoding="utf-8") as f:
                backup_content = f.read()
            with open(backup_file, "w", encoding="utf-8") as f:
                f.write(backup_content)

        with open(config_file, "w", encoding="utf-8") as f:
            f.write(content)

        return TestResult(
            success=True,
            message="Saved",
            data={"config_name": config_name, "config_type": config_type},
        )
    except Exception as e:
        return TestResult(success=False, message=f"Save failed: {str(e)}")
