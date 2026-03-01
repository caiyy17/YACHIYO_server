# SenseVoice ASR Wrapper

OpenAI Whisper-compatible API wrapper for [SenseVoice](https://github.com/FunAudioLLM/SenseVoice).

> **License**: SenseVoice is released under the [Apache 2.0 License](https://github.com/FunAudioLLM/SenseVoice/blob/main/LICENSE). Please comply with its license terms when using this wrapper.

## Setup

1. Clone SenseVoice to a sibling directory:

    ```bash
    cd ..  # parent of YACHIO_server
    git clone https://github.com/FunAudioLLM/SenseVoice.git
    ```

2. Create conda environment and install dependencies:

    ```bash
    conda create -n sensevoice python=3.10
    conda activate sensevoice
    cd SenseVoice
    pip install -r requirements.txt
    pip install -r ../YACHIO_server/Modules_standalone/SenseVoice/requirements_add.txt
    ```

3. Copy the wrapper file into the SenseVoice directory:
    ```bash
    cp ../YACHIO_server/Modules_standalone/SenseVoice/custom_sensevoice.py .
    ```

## Run

```bash
conda activate sensevoice
cd SenseVoice
python custom_sensevoice.py
# Listens on port 5052
```

## API

- `POST /v1/audio/transcriptions` — OpenAI Whisper API format
    - Input: multipart form with `file` (WAV audio) and optional `model` field
    - Output: `{"text": "transcribed text", "language": "zh"}`

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "asr": {
        "sensevoice_api": "http://127.0.0.1:5052/v1"
    }
}
```
