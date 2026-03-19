<div align="center">
  <img src="https://cdn.prod.website-files.com/69082c5061a39922df8ed3b6/6a0771283e71738dde179ec0_chrome-capture-2026-05-15.png" alt="Membase" width="100%" />
</div>

<br />

<div align="center">
  <img src="https://cdn.prod.website-files.com/69082c5061a39922df8ed3b6/6a077128b441515e51324513_SmUXbKB6_400x400%20(1).png" alt="Membase Logo" width="80" height="80" />

  <h1>Membase</h1>
  <p><strong>Your Personal Memory Layer for AI Agents</strong></p>

  <p>
    <a href="https://membase.so">
      <img src="https://img.shields.io/badge/Website-membase.so-0066FF?style=flat-square&logo=googlechrome&logoColor=white" />
    </a>
    <a href="https://x.com/Mem_base">
      <img src="https://img.shields.io/badge/Follow-%40Mem__base-000000?style=flat-square&logo=x&logoColor=white" />
    </a>
    <a href="https://x.com/JoshuaIPark">
      <img src="https://img.shields.io/badge/Follow-%40JoshuaIPark-000000?style=flat-square&logo=x&logoColor=white" />
    </a>
    <a href="https://discord.gg/vHgtDd6UTK">
      <img src="https://img.shields.io/badge/Discord-Join%20Community-5865F2?style=flat-square&logo=discord&logoColor=white" />
    </a>
    <a href="https://membase.so/pricing">
      <img src="https://img.shields.io/badge/Free%20Plan-Available-22c55e?style=flat-square&logo=checkmarx&logoColor=white" />
    </a>
    <img src="https://img.shields.io/badge/Status-Open%20Beta-f97316?style=flat-square" />
    <img src="https://img.shields.io/badge/Hackathon-2nd%20Place%20%F0%9F%A5%88-FFD700?style=flat-square" />
  </p>

  <p>
    <a href="https://membase.so">membase.so</a> ·
    <a href="https://membase.so/docs">Docs</a> ·
    <a href="https://membase.so/pricing">Pricing</a> ·
    <a href="https://membase.so/blog">Blog</a> ·
    <a href="https://discord.gg/vHgtDd6UTK">Community</a>
  </p>
</div>

---

## What is Membase?

AI agents are stateless by default. Every new session starts from zero — no memory of who you are, what you've worked on, or what decisions you've made. You re-explain yourself constantly. Context gets lost. Agents get dumber the more you use them across different tools.

**Membase fixes this.** It's a universal memory layer that sits between you and every AI agent you use. It automatically captures context from your conversations and connected apps, structures it into a living Knowledge Graph, and delivers the right context to any agent the moment it needs it — without you lifting a finger.

Your agents stop forgetting. They start knowing you.

---

## The Problem

| Problem | What Happens |
|---|---|
| **Session Amnesia** | Agents reset to zero when a session ends. Every conversation starts cold. |
| **Platform Isolation** | Context from ChatGPT never reaches Cursor. Cursor never reaches Claude. Each tool is its own silo. |
| **Signal-to-Noise** | Dumping raw context into every prompt degrades response quality. Agents need the *right* context, not all context. |

---

## How It Works

```
Connect → Capture → Route → Retrieve
```

**1. Connect** — Add Membase to any agent with a single MCP command. Works with Claude, ChatGPT, Cursor, VS Code, Codex, and more.

**2. Capture** — As you work, Membase automatically processes your conversations and pulls data from connected apps (Notion, Slack, Gmail, GitHub, Google Drive, Google Calendar) into your personal memory store.

**3. Route** — Incoming context is intelligently routed to the right store:
- **Memory** — Personal context, preferences, and history organized as a Knowledge Graph
- **Wiki** — Factual reference material as linked markdown documents

**4. Retrieve** — When an agent needs context, it calls `search_memory` or `search_wiki`. Membase returns the most relevant information using hybrid retrieval — not just what's recent, but what's actually useful.

---

## Features

### 🧠 Living Knowledge Graph
Your memory isn't a flat list of facts. Membase structures everything you know into a connected graph — nodes link to related nodes, so agents can traverse context the way you actually think. Every new conversation updates the graph automatically.

### ⚡ Context-Aware Agent Delivery
Membase delivers your context to every agent at runtime. Agents call `search_memory` and `search_wiki` to pull exactly what they need — reducing token bloat, improving response quality, and making every interaction feel personal.

### 🔄 Multi-Source Sync
Connect once, capture everywhere. Membase ingests from:
- **Chat history** — ChatGPT, Claude, Gemini
- **Productivity** — Notion, Slack, Google Drive, Gmail, Google Calendar
- **Dev tools** — GitHub
- **Knowledge bases** — Obsidian Vault import

### 🔌 MCP-Native
Built on the [Model Context Protocol](https://modelcontextprotocol.io). One line to integrate. Works with any MCP-compatible agent — no custom plugins, no per-tool setup.

### 🗄️ Hybrid Memory Retrieval
Membase doesn't do naive vector search. It combines:
- **Vector search** — semantic similarity
- **BM25** — keyword relevance
- **Graph relationships** — connected context traversal
- **Recency weighting** — recent events surface first
- **RRF (Reciprocal Rank Fusion)** — combines all signals into a single ranked result

This means agents get context that is semantically relevant, keyword-matched, structurally connected, and timely — all at once.

### 📋 Dual Memory Stores
| Store | Purpose | Format |
|---|---|---|
| **Memory** | Personal context, history, preferences | Knowledge Graph |
| **Wiki** | Reference material, facts, documentation | Linked Markdown |

---

## Supported Agents & Platforms

<table>
  <tr>
    <td align="center"><strong>ChatGPT</strong></td>
    <td align="center"><strong>Claude</strong></td>
    <td align="center"><strong>Gemini</strong></td>
    <td align="center"><strong>Cursor</strong></td>
    <td align="center"><strong>VS Code</strong></td>
  </tr>
  <tr>
    <td align="center"><strong>Codex</strong></td>
    <td align="center"><strong>OpenCode</strong></td>
    <td align="center"><strong>OpenClaw</strong></td>
    <td align="center"><strong>Poke</strong></td>
    <td align="center"><strong>Hermes</strong></td>
  </tr>
</table>

**Data Sources:** Notion · Slack · Google Drive · Gmail · Google Calendar · GitHub · Obsidian

---

## Quick Start

Add Membase to any MCP-compatible agent with a single command:

```bash
# Claude Desktop / Claude Code
# Add to your MCP settings:
{
  "mcpServers": {
    "membase": {
      "command": "npx",
      "args": ["-y", "@membase/mcp"]
    }
  }
}
```

Then connect your data sources and let Membase start building your Knowledge Graph automatically.

→ **[Full setup guide](https://membase.so/docs)**

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│                    Your AI Agents                    │
│         (Claude, ChatGPT, Cursor, VS Code...)        │
└────────────────────────┬────────────────────────────┘
                         │ MCP
                         ▼
┌─────────────────────────────────────────────────────┐
│                     Membase                          │
│                                                      │
│  ┌──────────────────┐    ┌────────────────────────┐ │
│  │   Memory Store   │    │      Wiki Store        │ │
│  │  Knowledge Graph │    │   Linked Markdown      │ │
│  │  search_memory() │    │    search_wiki()       │ │
│  └──────────────────┘    └────────────────────────┘ │
│                                                      │
│  Hybrid Retrieval: Vector + BM25 + Graph + RRF       │
└──────────────┬──────────────────────────────────────┘
               │ Auto-capture
               ▼
┌─────────────────────────────────────────────────────┐
│                   Your Data Sources                  │
│  Notion · Slack · Gmail · GitHub · Drive · Calendar  │
└─────────────────────────────────────────────────────┘
```

---

## Pricing

| | **Free** | **Pro** |
|---|---|---|
| **Price** | $0/month | $20/month (save 20% annually) |
| Memory Searches | Limited | Unlimited |
| Memory Episodes | Standard | 5× |
| Wiki Documents | Standard | 10× |
| AI Chats | Limited | 5× |
| AI Model Access | Standard | Advanced |
| MCP Integration | ✅ | ✅ |
| Chat History Import | ✅ | ✅ |
| Connected Apps | ✅ | ✅ |
| Model Training Opt-out | ✅ | ✅ |
| Support | Community | Priority |

→ **[View pricing](https://membase.so/pricing)**

---

## What People Are Saying

> *"This is the one memory layer that actually works across all my agents."*
>
> — **Ignacio Montenegro**, Entrepreneur

> *"It feels like a real Jarvis for my work, remembering context across projects."*
>
> — **Minsung Park**, CEO

---

## Recognition

🥈 **Claude Code Hackathon — 2nd Place** (November 3, 2025)

The Universal Memory MCP that became the foundation of Membase took 2nd place at Anthropic's Claude Code Hackathon. It's been evolving into what Membase is today ever since.

---

## Use Cases

**Founders** — Keep agents updated on your company context, investor relationships, and strategic decisions without re-explaining every session.

**Developers** — Give coding agents persistent knowledge of your codebase architecture, conventions, and past decisions.

**Researchers** — Build a second brain that agents can query. Import from Obsidian, structure with Wiki, retrieve with precision.

**Creators** — Let agents know your voice, style, past work, and audience without a prompt preamble every time.

**Investors** — Track deal flow, company updates, and portfolio context across every AI tool you use.

**Marketers** — Give agents the brand context, past campaigns, and audience knowledge they need to produce on-brand output instantly.

---

## Built By

<table>
  <tr>
    <td align="center">
      <a href="https://x.com/JoshuaIPark">
        <img src="https://cdn.prod.website-files.com/69082c5061a39922df8ed3b6/6a077128b441515e51324513_SmUXbKB6_400x400%20(1).png" width="60" height="60" style="border-radius:50%"><br />
        <strong>Joshua Park</strong><br />
        <sub>Co-founder & Head of Marketing</sub><br />
        <sub><a href="https://x.com/JoshuaIPark">@JoshuaIPark</a></sub>
      </a>
    </td>
  </tr>
</table>

---

## Community & Links

<p>
  <a href="https://membase.so"><strong>Website</strong></a> ·
  <a href="https://membase.so/docs"><strong>Docs</strong></a> ·
  <a href="https://membase.so/blog"><strong>Blog</strong></a> ·
  <a href="https://membase.so/pricing"><strong>Pricing</strong></a> ·
  <a href="https://discord.gg/vHgtDd6UTK"><strong>Discord</strong></a> ·
  <a href="https://x.com/Mem_base"><strong>@Mem_base</strong></a> ·
  <a href="https://linkedin.com/company/aristoapp"><strong>LinkedIn</strong></a> ·
  <a href="https://youtube.com/@aristo-so"><strong>YouTube</strong></a>
</p>

---

<div align="center">
  <sub>© 2026 Aristo Technologies, Inc. · <a href="https://membase.so/privacy">Privacy</a> · <a href="https://membase.so/terms">Terms</a> · support@aristo.so</sub>
</div>
