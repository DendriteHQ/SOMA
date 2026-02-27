# 🚀 Validator Setup Guide

> **Complete guide for setting up and running a validator node on the SOMA Subnet**

## 📋 Table of Contents

- [✅ Prerequisites](#-prerequisites)
- [💻 System Requirements](#-system-requirements)
- [📦 Installation](#-installation)
- [⚙️ Configuration](#️-configuration)
- [▶️ Running the Validator](#️-running-the-validator)
- [📊 Monitoring](#-monitoring)
- [🔧 Troubleshooting](#-troubleshooting)

---

## ✅ Prerequisites

### 🛠️ Required Software

- 🐍 **Python 3.11+** 
- 📝 **Git**
- 🔑 **Bittensor wallet** with registered hotkey on the subnet

### 👤 Required Accounts

> **⚠️ IMPORTANT:** You'll need the following accounts before proceeding:

- **Bittensor Wallet**: Registered and staked on netuid 114
- **LLM API Token**: For LLM-based scoring
  - 🔗 **OpenRouter**: Sign up at [openrouter.ai](https://openrouter.ai)

---

## 💻 System Requirements

> **📌 NOTE:** Minimum Recommended Specifications:

| Component | Requirement |
|-----------|-------------|
| 🖥️ **CPU** | 4 cores |
| 💾 **RAM** | 16 GB |
| 💽 **Storage** | 200 GB SSD |
| 🌐 **Network** | Stable internet with public IP |


---

## 📦 Installation

### 1️⃣ Install Python Dependencies

```bash
# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install subnet dependencies
pip install --upgrade pip
pip install -e .

# Install validator-specific dependencies
pip install -r reqs_tmp.txt

cd validator
```

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

## ▶️ Running the Validator

### 🔄 Using PM2 (Recommended)

> **💡 TIP:** PM2 is a production-ready process manager for Node.js applications

```bash
# Install PM2 (if not already installed)
npm install -g pm2

# 🚀 Start validator with auto-update watcher
cd /path/to/MCP-subnet/validator
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

#### 2. 🔑 **LLM API Token Issues**

**Problem:** LLM API authentication failures

**Diagnostic steps:**
```bash
# Verify token is set
env | grep OPENROUTER_API_TOKEN

# Test OpenRouter connectivity
curl https://openrouter.ai/api/v1/models \
  -H "Authorization: Bearer $OPENROUTER_API_TOKEN"
```

**Checklist:**
- [ ] Token is valid and not expired
- [ ] Account has sufficient credits
- [ ] Token has correct permissions
- [ ] No typos in `.env` file

---

#### 4. 🌐 **Platform Connection Failed**

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
