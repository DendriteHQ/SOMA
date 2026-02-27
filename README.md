# SOMA

![SOMA](docs/images/SOMA.jpg)

## Overview

This subnet brings **MCP (Model Context Protocol) servers** into the Bittensor ecosystem, enabling AI models to securely interact with external tools, data sources, and execution environments.

By combining MCP with Bittensor's incentive-driven design, the subnet creates a competitive environment where miners are rewarded for delivering **high-availability, low-latency, and high-quality MCP services**.


## Vision

Our goal is to build a decentralized platform of production-ready MCP servers that:

- extend AI models with real-world capabilities through standardized tool interfaces
- enable seamless integrations across systems, databases, and APIs
- support businesses, individual users, and other Bittensor subnets with reliable infrastructure
- continuously improve through competition, real usage metrics, and community feedback

The subnet aims to become the **standard for decentralized AI tooling infrastructure** - a universal platform designed for the entire Bittensor ecosystem and beyond. We're building flexible, production-ready MCP servers that any subnet, developer, or business can leverage, where quality and performance are rewarded through market-driven incentives.



## Architecture

### Platform

The Platform serves as the central orchestration layer of the subnet, responsible for:

- **Service Management**: Hosting and ensuring availability of MCP services
- **Algorithm Registry**: Managing and collecting models/algorithms submitted by miners
- **Analytics Dashboard**: Providing real-time performance metrics and insights
- **Miner Registration**: Processing and validating new miner submissions
- **Validation Orchestration**: Coordinating task distribution across validators
- **Quality Assessment**: Final evaluation of miner performance based on validator data

![Platform](docs/images/platform.png)

### Validators

Validators score miner solutions by:
- Fetching execution results from the platform
- Evaluatimg solution quality based on competition criteria
- Reporting scores to the platform for weight calculation

**Min Hardware Requirements:**
- 4 CPU cores
- 16 GB RAM
- 200 GB SSD storage

[**→ Validator Setup Guide**](docs/validator/validator-setup.md)

### Miners


On SOMA, any problem that can be meaningfully solved using an MCP server - and that can significantly improve agent performance - may become a competition target. Miners compete to deliver the most effective model or algorithm for a given task.


The miner’s responsibility is to design and implement model or algorithm that solves the defined problem as effectively as possible and upload it to the platform

**All a miner needs to participate is:**
- A working algorithm that solves the active MCP task
- A registered hotkey on netuid 114

The platform handles orchestration and evaluation. Validators automatically retrieve submitted solutions associated with registered hotkeys and score them according to the active competition criteria.

[**→ Miner Setup Guide**](docs/miner/miner-setup.md)




## First MCP competition: Context Compression

The inaugural MCP challenge focuses on **context compression** - a critical problem for reducing costs and improving quality in agentic AI systems.

### Why context compression?
- Lower inference costs
- Faster responses
- Longer effective memory windows
- Stronger reasoning from distilled context
- Scalable intelligence for multi-agent systems

[**→ Context Compression**](docs/miner/context-compression.md)



## Incentive Mechanism

The subnet operates on a **recurring weekly competition cycle** using a winner-takes-all incentive model.

The platform aggregates validator assessments into final rankings, and validators set on-chain weights based on platform-calculated scores.

Each miner may upload **one algorithm per hotkey per competition cycle**.

The primary objective of the incentive mechanism is to reward:

- **State-of-the-art algorithmic quality**
- Solutions aligned with best engineering practices
- Robust, generalizable approaches rather than overfitted heuristics
- Meaningful innovation that advances the field
---

### Weekly Competition Cycle

Each competition lasts **one week** and consists of three distinct phases:

#### 1️⃣ Submission Window

- Miners upload or update their algorithms on the platform
- Submissions must be associated with a registered hotkey
- Only code submitted during this window is eligible for the current cycle

#### 2️⃣ Screening & Qualification Phase (During Submission Window)

- Submitted solutions undergo automated validation and integrity checks
- The platform evaluates baseline performance and technical correctness
- A subset of **top-performing qualified submissions** is selected to advance to the main competition phase

This screening stage ensures:
- Stability and security of evaluated solutions
- Minimum quality standards
- Efficient allocation of validator resources

#### 3️⃣ Competition Phase

- Qualified solutions are evaluated continuously under live competition conditions
- Validators score miners according to the active task criteria
- Final rankings are computed at the end of the weekly cycle

---

### Reward Distribution

- The top-ranked miner is declared the weekly winner
- The winner receives the subnet incentive allocation
- After the reward period ends, a new weekly competition cycle begins

---

This structure:

- Encourages rapid iteration and continuous improvement
- Prevents stagnant dominance without sustained performance
- Ensures only stable and high-quality solutions reach the competitive stage
- Creates predictable and recurring incentive cycles




## Cross-Subnet Integrations

SOMA is designed for seamless integration with other Bittensor subnets.

> **Coming Soon**: Details about confirmed partnerships and integrations across subnets.




## Community & Governance

This subnet is **community-driven** and values transparency and collaboration:

- **Cross-Subnet Partnerships**: Actively seeking integrations with complementary subnets
- **Community Voting**: Future governance model for selecting new MCP server types
- **Bug Bounty Program**: Planned initiative to ensure validation integrity and security
- **Open Development**: Public roadmap and regular updates on subnet evolution

### Get Involved

- Join our Discord for technical discussions
- Contribute to the GitHub repository
- Propose new MCP server types and use cases
- Participate in testing and feedback cycles


## Contact

- **Discord**: [Join our server](https://discord.com/invite/durr4Sg6sM)
- **Twitter**: [@SomaSubnet](https://x.com/SomaSubnet)
- **GitHub**: [github.com/DendriteHQ/SOMA](https://github.com/DendriteHQ/SOMA)
- **Email**:  thesoma@dendrite.holdings

---

<div align="center">

**Built on [Bittensor](https://bittensor.com) | Powered by the Community**

</div>










