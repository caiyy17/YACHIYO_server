# YACHIYO Web UI

Web management interface for YACHIYO server. Provides client management, pipeline configuration, and log viewing.

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
- **Pipeline Viewer** (`/pipeline-editor`): READ-ONLY graph view of one config —
  pick a server config from the dropdown (or load a local JSON file). Shows
  per-node vars, signal declarations (with declaration-derived flow lines;
  an undeclared hop is drawn red), module params and `next_nodes`. Data
  wires are matched by variable name; wire-side JSON `null` (the config
  protocol's explicit opt-out) is displayed as the literal text `null`.
  No editing of any kind — configs are edited by hand (or via the raw
  Config Editor above for `*_tmp.json`).

## File Structure

```
webui/
├── web_ui.py               # FastAPI backend, proxies requests to main server
├── pipeline_editor.html    # Read-only pipeline viewer (self-contained page)
├── templates/
│   └── index.html          # Single-page frontend
├── requirements.txt
└── README.md
```
