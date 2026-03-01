import torch
from funasr import AutoModel
# from funasr.utils.postprocess_utils import rich_transcription_postprocess

from flask import Flask, request, jsonify, Response
import json
import time
import os
import re
import wave

app = Flask(__name__)

model_dir = "iic/SenseVoiceSmall"
device = "cuda" if torch.cuda.is_available() else "cpu"
print(f"Using device: {device}")
model = AutoModel(
    model=model_dir,
    vad_model="fsmn-vad",
    vad_kwargs={"max_single_segment_time": 30000},
    trust_remote_code=True,
    device=device,
    disable_update=True,
)

LANGUAGES = ["<|zh|>", "<|en|>", "<|yue|>", "<|ja|>", "<|ko|>"]
LANG_MAP = {
    "<|zh|>": "zh",
    "<|en|>": "en",
    "<|yue|>": "yue",
    "<|ja|>": "ja",
    "<|ko|>": "ko",
}

from collections import Counter


def find_most_common_substring(s, substrings):
    # Count occurrences of each substring in the string
    counts = Counter({substring: s.count(substring) for substring in substrings})

    # Find the most frequent substring and its count
    max_count = max(counts.values())
    most_common = [
        substring for substring in substrings if counts[substring] == max_count
    ]

    # Return the first one according to list order
    return most_common[0], max_count



@app.route("/v1/audio/transcriptions", methods=["POST"])
def openai_transcriptions():
    """OpenAI-compatible Whisper API endpoint."""
    start = time.time()
    if "file" not in request.files:
        return jsonify({"error": {"message": "No file provided", "type": "invalid_request_error"}}), 400

    file = request.files["file"]
    response_format = request.form.get("response_format", "json")

    time_stamp = time.strftime("%Y%m%d%H%M%S", time.localtime())
    filename = "tmp/received_file_" + time_stamp + "_oai.wav"
    file.save(filename)

    # Get audio duration
    duration = 0.0
    try:
        with wave.open(filename, "rb") as wf:
            duration = wf.getnframes() / float(wf.getframerate())
    except Exception:
        pass

    res = model.generate(
        input=(filename),
        cache={},
        language="auto",
        use_itn=False,
        batch_size_s=0,
    )
    text = res[0]["text"]
    lang, _ = find_most_common_substring(text, LANGUAGES)
    text = re.sub(r"<\|.*?\|>", "", text)
    lang_code = LANG_MAP.get(lang, "auto")

    print(lang_code + ": ", text)
    print("Time:", time.time() - start)

    if response_format == "text":
        return Response(text, mimetype="text/plain")
    elif response_format == "verbose_json":
        return jsonify({"text": text, "language": lang_code, "duration": duration})
    else:  # "json" (default)
        return jsonify({"text": text})


if __name__ == "__main__":
    # Remove the tmp directory
    if os.path.exists("tmp"):
        os.system("rm -rf tmp")
    os.makedirs("tmp")

    filename = "test_voice.wav"
    res = model.generate(
        input=(filename),
        cache={},
        language="zh",  # "zn", "en", "yue", "ja", "ko", "nospeech"
        use_itn=False,
        batch_size_s=0,
    )
    print(res[0]["text"])

    app.run(debug=False, host="0.0.0.0", port=5052)
