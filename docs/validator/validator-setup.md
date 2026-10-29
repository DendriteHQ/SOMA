# 🚀 Validator Setup Guide

> **Complete guide for setting up and running a validator node on the SOMA Subnet**

## 📋 Table of Contents

- [✅ Prerequisites](#-prerequisites)
- [💻 System Requirements](#-system-requirements)
- [📦 Installation](#-installation)
- [⚙️ Configuration](#️-configuration)
- [🧪 What the validator grades](#-what-the-validator-grades)
- [▶️ Running the Validator](#️-running-the-validator)
  - [🐳 Using Docker](#-using-docker-recommended)
- [📊 Monitoring](#-monitoring)
- [🔧 Troubleshooting](#-troubleshooting)

---

## ✅ Prerequisites

### 🛠️ Required Software

- 🐍 **Python 3.11+** 
- 📝 **Git**
- 🐳 **Docker** — grading runs the task's own container. No registry login is needed:
  every image the validator pulls is public at the time it needs it.
- 🔑 **Bittensor wallet** with registered hotkey on the subnet

### 👤 Required Accounts

> **⚠️ IMPORTANT:** You'll need the following accounts before proceeding:

- **Bittensor Wallet**: Registered and staked on netuid 114
---

## 💻 System Requirements

> **📌 NOTE:** Minimum Recommended Specifications:

| Component | Requirement |
|-----------|-------------|
| 🖥️ **CPU** | 4 cores |
| 💾 **RAM** | 16 GB |
| 💽 **Storage** | 500 GB SSD |
| 🌐 **Network** | Stable internet with public IP |


---

## 📦 Installation

### 1️⃣ Install Python Dependencies

```bash
# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies (from the repository root)
pip install --upgrade pip
pip install -r requirements.txt

cd validator
```

> **💡 TIP:** Running in Docker needs no Python setup at all — see
> [🐳 Using Docker](#-using-docker-recommended).

---

## ⚙️ Configuration

> **⚠️ IMPORTANT:** Setup your environment file before running the validator

📄 Copy the example configuration file and edit with your values:

```bash
cd validator
cp .env.example .env
nano .env  # or use your preferred editor
```

---

## 🧪 What the validator grades

Every run is scored the same way — did the agent's patch make the task's tests pass —
but a competition draws its tasks from two places, and each has its own grading path.
Both are built in; there is nothing to choose or configure.

| Stage | Tasks from | Graded by | Images pulled from |
|---|---|---|---|
| Screener stage 1 | `SWE-bench/SWE-bench_Verified` | the SWE-bench harness | `ghcr.io/epoch-research/...` (public) |
| Screener stage 2, evaluation | SOMA task lists | the task's own test image | `dendritexhq/soma-competition-tasks-dind` |

The platform names the dataset on each validation task, and the validator routes on it.

**SOMA tasks.** These are not a Hugging Face dataset. Each task ships a *test image*
holding the repository at `base_commit` with the task's test patch already applied, its
dependencies installed, and a `run_tests` entrypoint. Grading is the container
equivalent of the harness: pull the image, start it with **no network**, copy the
miner's patch in, apply it, run `run_tests`, and check the task's `FAIL_TO_PASS` /
`PASS_TO_PASS` ids against the pytest JSON report it produces. Where to run and what to
run come from the image's own `soma.*` labels.

**No registry credentials needed.** Those images live in a private Docker Hub
repository so a competition's hidden tasks are not published in advance. Rather than
distributing a shared registry token to every validator, the platform makes the
repository public for the evaluation window and private again once it closes.

That repository holds exactly the current competition's tasks: the platform mirrors the
images of the tasks the competition actually references into it and removes the rest, so
a tag you can pull there is a task that is being scored right now. Tags from past
competitions disappear once their window has closed.

---

## ▶️ Running the Validator

### 🐳 Using Docker (Recommended)

Requires only Docker with the Compose plugin and your wallet's hotkey on the host.

```bash
cd /path/to/SOMA/validator
cp .env.example .env
nano .env   # WALLET_NAME, WALLET_HOTKEY, VALIDATOR_HOST (your public IP)

docker compose up -d        # build and start (also restarts on reboot)
docker compose logs -f      # live logs
docker compose ps           # status and health
docker compose down         # stop
```

The image is built locally from your checkout; `up` rebuilds it (cached layers make
that quick when nothing changed). The heartbeat reports the commit the image was built
from.

**Updating:** `git pull`, then the same `up` command. Nothing is fetched or installed
inside a running container, and there is no automatic update as with the PM2 watcher.

What the compose file sets up:

| Mount | Why |
|---|---|
| `/var/run/docker.sock` | Graded task containers run as siblings on the host's Docker daemon. **This socket is root-equivalent on the host** — run the validator on a machine dedicated to it. |
| `BT_WALLET_PATH` (default `~/.bittensor/wallets`), read-only | Only the hotkey is used, for signing and setting weights. Keep the coldkey off this host; `coldkeypub.txt` and `hotkeys/<name>` are enough. |
| volumes `state`, `tasks`, `logs`, `hf-cache` | Last set weights, current competition, the cached task dataset, harness logs and the Hugging Face cache survive restarts and upgrades. |

The container listens on `VALIDATOR_PORT` and publishes the same port, because that is
the port advertised to the platform — keep it reachable from the internet. Grading
containers left behind by a stop, restart or crash are removed when the validator next
starts. The heartbeat reports the image's version rather than a git commit.

### 🔄 Using PM2

> **💡 TIP:** PM2 is a production-ready process manager for Node.js applications

```bash
# Install PM2 (if not already installed)
npm install -g pm2

# 🚀 Start validator with auto-update watcher
cd /path/to/SOMA/validator
pm2 start run_validator.sh --name mcp-validator-watch --interpreter bash -- 60

# 🔄 Auto-restart on system reboot
pm2 startup
pm2 save

# 📊 View status
pm2 status

# 🛑 Stop watcher (and validator will no longer be restarted)
pm2 stop mcp-validator-watch

# 🔁 Restart watcher
pm2 restart mcp-validator-watch
```

> **ℹ️ Note:** `run_validator.sh` checks the repo for new commits, fast-forwards, and restarts the validator process when updates are found. It runs the actual validator in a separate PM2 process named `mcp-validator`, so when troubleshooting you may need to check logs for both `mcp-validator-watch` (the watcher) and `mcp-validator` (the validator itself).

---

## 📊 Monitoring

### 📜 Check Validator Logs

With Docker: `docker compose logs -f validator`. With PM2:

```bash
# 📖 View live logs
pm2 logs mcp-validator

# 📄 View specific log file
tail -f ~/.pm2/logs/mcp-validator-out.log
tail -f ~/.pm2/logs/mcp-validator-error.log

# 🔍 Search logs for errors
grep -i error ~/.pm2/logs/mcp-validator-error.log
```

### 🎯 Key Metrics to Monitor

✅ **Healthy Indicators:**
- ✔️ Successful task fetches from platform
- ✔️ Regular score submissions to platform
- ✔️ Consistent weight setting

⚠️ **Warning Signs:**
- ❌ LLM API errors or timeouts
- ❌ Platform connection failures
- ❌ Memory/CPU exhaustion

---

## 🔧 Troubleshooting

### 🐛 Common Issues


#### 1. ⏸️ **No Tasks Available (503 Error)**

**Problem:** Platform returns 503 "No tasks available"

> **📌 NOTE:** This is **normal behavior** when all miners have been scored

**Expected behavior:**
```
INFO: No tasks available (attempt 1), backing off to 30.0s poll interval
```

**What to do:**
- ✅ Wait for new miner submissions
- ✅ Let the validator auto-retry with backoff
- ✅ Check platform status

---

#### 2. 🔒 **`pull access denied` for a task test image**

**Problem:** The hidden-task repository is still private.

> **📌 NOTE:** The platform makes the repository public when the evaluation window opens
> and private again once it closes, and reconciles that on an interval — so a single
> failure right at the boundary is normal, and the validation is simply retried.

**What to do:**
- ✅ Ignore an isolated occurrence in the first minutes of the evaluation window
- ✅ If it persists, report it — the platform-side visibility flip may be failing, and
  no validator will be able to grade that competition's tasks until it is fixed

A `manifest unknown` for a single task is a different problem: the repository is
reachable but that task's image was never copied into it. Report the instance id —
the platform holds such tasks back from dispatch, so a run existing for one means
something copied only partially.

---

#### 3. 🌐 **Platform Connection Failed**

**Problem:** Can't connect to platform API

**Checklist:**
- [ ] Platform URL is correct in `.env`
- [ ] Platform signer SS58 is correct
- [ ] Network connectivity to platform
- [ ] Platform is online and accepting requests


---

## 🎉 Success!

**Your validator is now running! 🚀**

> **💡 TIP:** Join our community channels for support and updates!

**Next steps:**
- 📊 Monitor your validator's performance
- 💬 Join the community Discord
- 📚 Read the full documentation
- 🔄 Keep your validator updated

---

<div align="center">

**Good luck validating! 💪**

Made with ❤️ for the SOMA Subnet

</div>
