# BertVITS2 TTS Wrapper

OpenAI TTS-compatible API wrapper for [Bert-VITS2](https://github.com/fishaudio/Bert-VITS2).

> **License**: Bert-VITS2 is released under the [AGPL-3.0 License](https://github.com/fishaudio/Bert-VITS2/blob/master/LICENSE). Please comply with its license terms when using this wrapper.

## Setup

1. Clone Bert-VITS2 to a sibling directory:

    ```bash
    cd ..  # parent of YACHIO_server
    git clone https://github.com/fishaudio/Bert-VITS2.git
    ```

2. Create conda environment and install dependencies:

    ```bash
    conda create -n bertvits python=3.10
    conda activate bertvits
    cd Bert-VITS2
    pip install -r requirements.txt
    pip install -r ../YACHIO_server/Modules_standalone/BertVITS2/requirements_add.txt
    ```

3. Copy the wrapper file and voice configs into the Bert-VITS2 directory:

    ```bash
    cp ../YACHIO_server/Modules_standalone/BertVITS2/custom_bertvits.py .
    cp ../YACHIO_server/Modules_standalone/BertVITS2/yml_configs/*.yml yml_configs/
    ```

4. Place your trained voice model files according to the yml config (see `yml_configs/` for examples).

## Run

```bash
conda activate bertvits
cd Bert-VITS2
python custom_bertvits.py
# Listens on port 9880
```

## API

- `POST /v1/audio/speech` — OpenAI TTS API format
    - Input: JSON `{"model": "<voice_config_name>", "input": "text to synthesize"}`
    - Output: WAV audio bytes
    - The `model` field selects the voice config from `yml_configs/<model>.yml`

## Configuration

Service address is configured in `configs/settings/settings.json`:

```json
{
    "tts": {
        "bertvits_api": "http://127.0.0.1:9880/v1"
    }
}
```
