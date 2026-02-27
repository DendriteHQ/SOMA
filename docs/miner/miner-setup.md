# ⛏️ Miner Setup Guide

> **Complete guide for developing and submitting solutions to SOMA Subnet**

## 📋 Table of Contents

- [✅ Prerequisites](#-prerequisites)
- [🎯 Competition Overview](#-competition-overview)
- [💻 Development Setup](#-development-setup)
- [📝 Writing Your Solution](#-writing-your-solution)
- [🚀 Uploading to Platform](#-uploading-to-platform)

---

## ✅ Prerequisites


### 👤 Required Accounts

> **⚠️ IMPORTANT:** You need a Bittensor wallet registered on the subnet

- **Mainnet (netuid 114)** - SOMA Subnet

### 💰 Registration

```bash
# Create wallet if you don't have one
btcli wallet new_coldkey --wallet.name <your_wallet>
btcli wallet new_hotkey --wallet.name <your_wallet> --wallet.hotkey <your_hotkey>

# Or register on mainnet (requires stake)
btcli subnet register --netuid 114 --wallet.name <your_wallet> --wallet.hotkey <your_hotkey>
```

---

## 🎯 Competition Overview

### 🏆 What You're Building

Miners participate in **various competitions** with different challenges and themes. Your solutions will:

1. 📥 Receive competition-specific tasks and requirements
2. 🔧 Implement solution that solve the given challenge
3. 🏆 Compete for the **best performing solution**
4. 🚀 **Winning solutions become MCP (Model Context Protocol) servers**

---


## 📝 Writing Your Solution

### 🏗️ Solution Structure

Your solution must be a Python function with a `main()` entry point. The signature depends on the specific competition:

```python
def main(*args, **kwargs):
    """
    Main entry point for your solution.
    
    Args and return types vary by competition.
    Check the specific competition requirements.
    """
    # Your solution logic here
    result = your_algorithm(*args, **kwargs)
    return result
```

> **📋 Note:** Always check the specific competition documentation for exact input/output requirements.



---

### 🔍 Validation Checklist


Before uploading, verify:
- [ ] 🚫 **You MUST NOT upload obfuscated code**
- [ ] ✅ Your algorithm meets competition-specific requirements
- [ ] ✅ Solution works correctly on test cases
- [ ] ✅ All dependencies are properly imported
- [ ] ✅ Code follows competition interface requirements
---

## 🚀 Uploading to Platform

### 📦 Prepare Your Solution

1. **Save your solution** to a Python file (e.g., `my_solution.py`)
2. **Ensure all imports** are at the top
3. **Verify it meets** the competition's specific requirements

### 🎯 Using the Upload Script

The repository includes a convenient upload script:


**Location:** `miner/upload_miner.py`


#### ▶️ Running the Upload Script

```bash
# Activate your virtual environment
source .venv/bin/activate

# Run the upload script
cd miner
python3 upload_miner.py 
```

**Expected output:**

```
Loading wallet: my_wallet/my_hotkey
Miner hotkey: 5CXXXXXXXXXXXXXXXXxxx
Loaded solution code (2543 bytes)

Sending upload request to http://platform_url:port/miner/upload
Nonce: abc123...
Signature: def456...

Response status: 200
Upload successful: True
Response signature: ghi789...
```

---

## 🎉 Next Steps

**You're ready to compete! 🏆**

**Recommended workflow:**

1. ✅ **Develop** - Build your solution for the current competition
2. 🧪 **Test** - Validate locally with test tasks provided by SOMA
3. 📊 **Monitor** - Check scores and identify improvements
4. 🔄 **Iterate** - Refine based on feedback
5. 🚀 **Deploy to mainnet** - Upload final version to netuid 114
6. 🏆 **Compete** - Aim for top ranking - winners become MCP servers!

---

<div align="center">

**Happy Mining! ⛏️💎**

Made with ❤️ for the SOMA Subnet

*Questions? Join our [Discord](https://discord.com/invite/durr4Sg6sM)*

</div>
