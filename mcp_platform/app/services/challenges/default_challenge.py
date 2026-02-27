import json
from pathlib import Path


def main(task: str) -> str:
    words = task.strip().split()
    if words:
        words = words[:-1]
    compressed = " ".join(words)
    output_path = Path("/sandbox/output/output.json")
    output_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        payload = json.loads(output_path.read_text())
    except Exception:
        payload = {}
    payload["compressed"] = compressed
    output_path.write_text(json.dumps(payload) + "\n")
    return compressed
