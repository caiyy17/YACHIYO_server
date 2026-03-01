# Vector Database Server

BGE embedding + FAISS-based vector similarity search server for RAG action matching and lorebook activation.

> **Dependencies**: Uses [sentence-transformers](https://github.com/UKPLab/sentence-transformers) (Apache 2.0), [FAISS](https://github.com/facebookresearch/faiss) (MIT), and the [BGE-M3](https://huggingface.co/BAAI/bge-m3) model (MIT). Please comply with their respective licenses.

## Dependencies

- `flask` — HTTP server
- `sentence_transformers` — BGE-M3 embedding model
- `faiss` — vector similarity search (requires conda install)
- `torch` (CUDA) — GPU acceleration for embedding (falls back to CPU but significantly slower)

The BGE-M3 model (`BAAI/bge-m3`) is downloaded automatically on first run.

## Setup

```bash
conda create -n database python=3.10
conda activate database
pip install torch --index-url https://download.pytorch.org/whl/cu126  # CUDA version
pip install -r requirements.txt
conda install -c conda-forge faiss-gpu
```

## Run

```bash
conda activate database
python database_server.py
# Listens on port 5054
```

## Configuration

Service address and dataset path are configured in `configs/settings/settings.json`:

```json
{
    "data_query": {
        "addr_data_query": "http://127.0.0.1:5054",
        "datasets_path": "configs/datasets/"
    }
}
```
