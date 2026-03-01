# YACHIO Web UI

Web management interface for YACHIO server. Provides client management, pipeline configuration, and log viewing.

## Quick Start

```bash
# Make sure the main server is running first
cd webui && uvicorn web_ui:app --host 0.0.0.0 --port 8001
```

Open http://localhost:8001 in browser.

## Features

- **Client Management**: Register/unregister clients, view client list and status
- **Pipeline Control**: Select config and initialize pipeline for a client
- **Config Viewer/Editor**: Browse and edit config files (only `*_tmp.json` files are editable)
- **Log Viewer**: View client logs with auto-refresh

## File Structure

```
webui/
├── web_ui.py          # FastAPI backend, proxies requests to main server
├── templates/
│   └── index.html     # Single-page frontend
├── requirements.txt
└── README.md
```
