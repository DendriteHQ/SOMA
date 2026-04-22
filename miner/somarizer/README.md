# SOMArizer Miner API Guide

This guide is synced to the live SOMArizer docs (`https://somarizer.thesoma.ai/docs#/`) and OpenAPI spec (`https://somarizer.thesoma.ai/openapi.json`).

Base URL: `https://somarizer.thesoma.ai`

---

## Endpoints

| Method | Path | Auth | Purpose |
| --- | --- | --- | --- |
| `POST` | `/auth/miner-key` | Signed hotkey payload | Issue a miner API key |
| `POST` | `/summarize` | Bearer token | Summarize text/PDF to a target compression ratio |

Notes:
- `/auth/miner-key` issues a fresh key per call and revokes the previous key for the same hotkey.
- `/summarize` expects `multipart/form-data`.
- Miner API keys are rate-limited per key: `5` requests per minute and `50` requests per day.

---

## Quick Start

### Script workflow (recommended)

The `miner` folder includes:
- `request_miner_key.py` - requests your personal API key from your hotkey signature.
- `somarizer_test.py` - tests `/summarize` with either text or a PDF file.

1) Request a key:

```bash
cd /root/SOMA/miner
python3 request_miner_key.py --hotkey <PATH_TO_HOTKEY>
```

2) Save the returned key:

```bash
export SOMA_MINER_API_KEY="<API_KEY>"
```

3) Run a text test:

```bash
python3 somarizer_test.py \
  --text "Paste between 200 and 100000 characters of text here" \
  --compression-ratio 0.25
```

4) Run a PDF test:

```bash
python3 somarizer_test.py \
  --pdf /absolute/path/to/input.pdf \
  --compression-ratio 0.25
```

---

### 1) Create a signed miner-key request

`POST /auth/miner-key` expects:
- `public_key`: your hotkey SS58 address
- `nonce`: format `YYYYMMDDTHHMMSSffffffZ.hex32`
- `signature`: base64 signature of `payload:somarizer:issue_miner_key:{public_key_ss58}::nonce:{nonce}`

Example signing snippet:

```python
import base64
import bittensor as bt
from soma_shared.utils.signer import generate_nonce

wallet = bt.Wallet(name="your_wallet_name", hotkey="your_hotkey_name")
public_key_ss58 = wallet.hotkey.ss58_address
nonce = generate_nonce()
message = f"payload:somarizer:issue_miner_key:{public_key_ss58}::nonce:{nonce}".encode("utf-8")
signature = base64.b64encode(wallet.hotkey.sign(message)).decode("utf-8")

print(public_key_ss58)
print(nonce)
print(signature)
```

Use those values in the request:

```bash
curl -sS -X POST https://somarizer.thesoma.ai/auth/miner-key \
  -H "Content-Type: application/json" \
  -d '{
    "public_key": "<HOTKEY_SS58>",
    "nonce": "<NONCE>",
    "signature": "<BASE64_SIGNATURE>"
  }'
```

Successful response returns:

```json
{
  "api_key": "soma_...or soma_miner_...",
  "hotkey": "<HOTKEY_SS58>"
}
```

Save the key:

```bash
export SOMA_MINER_API_KEY="<API_KEY>"
```

### 2) Summarize text

```bash
curl -sS -X POST https://somarizer.thesoma.ai/summarize \
  -H "Authorization: Bearer $SOMA_MINER_API_KEY" \
  -F "compression_ratio=0.25" \
  -F "text=Paste between 200 and 100000 characters of text here"
```

### 3) Summarize a PDF

```bash
curl -sS -X POST https://somarizer.thesoma.ai/summarize \
  -H "Authorization: Bearer $SOMA_MINER_API_KEY" \
  -F "compression_ratio=0.25" \
  -F "file=@/absolute/path/to/input.pdf;type=application/pdf"
```

---

## Request Parameters (`POST /summarize`)

| Parameter | Type | Required | Description |
| --- | --- | --- | --- |
| `compression_ratio` | number | Yes | Target ratio in range `0.2` to `0.9` |
| `text` | string | No | Inline text to summarize (minimum `200`, maximum `100000` characters) |
| `file` | binary | No | Uploaded PDF content to summarize |

In practice, provide at least one input source (`text` or `pdf file`) with `compression_ratio`. If you use text input (`text` or `--text-file`), ensure it is between `200` and `100000` characters.

---

## Response Format (`200`)

```json
{
  "summary": "compressed output",
  "input_length": 1234,
  "output_length": 309
}
```

---

## Common Errors

| Status | Where | Meaning |
| --- | --- | --- |
| `400` | `/auth/miner-key` | Invalid nonce/signature/request details |
| `401` | `/summarize` | Missing or invalid bearer token |
| `422` | both POST endpoints | Validation error (missing/invalid fields) |

---

## Security Reminders

- Protect your API key like wallet credentials.
- Do not commit keys to git. Use environment variables or a gitignored `.env`.
- Because keys rotate on re-issue, update stored secrets after calling `/auth/miner-key` again.

---

## Support

Questions, issues, or suspicious behavior should be reported in SOMA community channels.
