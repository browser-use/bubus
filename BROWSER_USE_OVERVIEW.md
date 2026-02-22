# Browser Use: Organization Overview

This document provides a comprehensive overview of the **browser-use** GitHub organization, its mission, architecture, and all repositories. It is written as a briefing for an agent that needs full context on what Browser Use is and how everything fits together.

---

## What Is Browser Use?

Browser Use is an **open-source AI agent framework** that enables large language models (LLMs) to autonomously control web browsers. It translates natural language instructions into real browser actions -- clicking, typing, navigating, extracting data, filling forms -- allowing AI agents to automate any task a human could perform on the web.

**Core thesis:** Websites were built for humans. Browser Use makes them accessible for AI agents.

**Founded:** October 2024 by Gregor Zunic. MIT licensed. 78,000+ GitHub stars.

---

## How It Works (Architecture)

### The Agent Loop

1. The user provides a **task** in natural language (e.g., "Find the cheapest flight from NYC to London next Friday").
2. The **Agent** class receives the task, an **LLM** (the brain), and a **Browser** instance (the hands).
3. Each iteration of the agent loop:
   - The browser's current state (DOM, clickable elements, screenshots) is extracted and sent to the LLM.
   - The LLM decides the next action (click element X, type "text" into field Y, scroll, navigate to URL, etc.).
   - The action is executed in the browser via Playwright/CDP.
   - The cycle repeats until the task is complete or the agent determines it cannot proceed.
4. Results are returned to the caller.

### Key Components

| Component | Role |
|---|---|
| **Agent** | Orchestrates the task loop. Takes a task string, an LLM, a Browser, and optional custom tools. Runs asynchronously. |
| **Browser** | Manages a Chromium instance (local or cloud). Handles page navigation, element interaction, screenshots, and state extraction. |
| **LLM** | The decision-making model. Supports ChatBrowserUse (optimized for browser tasks), Google Gemini, Anthropic Claude, OpenAI, DeepSeek, Ollama (local), and others via LangChain. |
| **DOM Extractor** | Processes the page DOM to identify interactive elements, labels, and structure. Feeds a simplified, actionable representation to the LLM. |
| **Action Space** | The set of actions the agent can take: click, type, scroll, navigate, go_back, screenshot, extract_data, wait, and custom tool actions. |
| **Custom Tools** | Developers extend the agent with domain-specific actions using `@tools.action()` decorators. |

### Minimal Code Example

```python
from browser_use import Agent, Browser
from langchain_anthropic import ChatAnthropic

agent = Agent(
    task="Find the best price for a MacBook Air on Amazon",
    llm=ChatAnthropic(model="claude-sonnet-4-20250514"),
    browser=Browser(),
)
result = await agent.run()
```

---

## Supported LLMs

| Provider | Model Class | Notes |
|---|---|---|
| Browser Use | `ChatBrowserUse()` | Optimized for browser tasks. 3-5x faster than general models. $0.20/1M input, $2.00/1M output tokens. |
| Anthropic | `ChatAnthropic()` | Claude models |
| Google | `ChatGoogle()` | Gemini models |
| OpenAI | `ChatOpenAI()` | GPT-4o, GPT-4, etc. |
| DeepSeek | `ChatDeepSeek()` | DeepSeek-R1 for advanced reasoning |
| Ollama | Local models | For fully local/private operation |
| Azure OpenAI | `AzureChatOpenAI()` | Enterprise Azure deployments |

---

## Cloud vs Local

- **Local:** Default. Runs a local Chromium instance via Playwright. Free, full control, but no stealth/proxy features.
- **Cloud (`Browser(use_cloud=True)`):** Browser Use Cloud provides stealth-enabled browsers with proxy rotation, CAPTCHA avoidance, fingerprint management, memory management, and parallel execution for production workloads. Requires an API key.

---

## All Repositories

The browser-use organization contains 19 repositories. They fall into these categories:

### Core Framework

#### [`browser-use`](https://github.com/browser-use/browser-use) -- The Main Library
- **What:** The core Python framework. This is the library everything else is built on.
- **Language:** Python (98%) | **Stars:** 78,700+ | **License:** MIT
- **Install:** `pip install browser-use` or `uv add browser-use`
- **Python:** >=3.11
- **Key dependencies:** Playwright (browser automation), LangChain (LLM abstraction), Pydantic (data models), aiohttp/httpx (HTTP)
- **CLI:** Ships with a CLI (`browser-use` / `bu`) for interactive browser control, navigation, screenshots, and state inspection.
- **Entry points:** `browser-use`, `browseruse`, `bu`, `browser` (CLI), `browser-use-tui` (legacy terminal UI)

#### [`cdp-use`](https://github.com/browser-use/cdp-use) -- Type-Safe CDP Client
- **What:** A type-safe Python client generator for the Chrome DevTools Protocol. Auto-generates Python bindings from Chrome's official CDP spec with full TypedDict type hints, event registration, and IDE autocomplete.
- **Language:** Python | **Stars:** 251 | **License:** MIT
- **Why it matters:** This is the low-level protocol layer. browser-use uses CDP to communicate with Chrome/Chromium. cdp-use provides the type-safe interface.
- **Key feature:** Run `python -m cdp_use.generator` to regenerate bindings from the latest CDP spec. 50+ domain modules (DOM, Network, Page, Runtime, etc.).

#### [`bubus`](https://github.com/browser-use/bubus) -- Event Bus Library
- **What:** Production-ready async/sync event bus for Python. Powers the event-driven internals of browser-use.
- **Language:** Python | **Stars:** 95 | **License:** MIT | **Version:** 1.5.6
- **Key features:**
  - Pydantic-based typed events with validation
  - Async-first with FIFO processing
  - Handler registration by event class, string name, or wildcard (`'*'`)
  - Nested event handling with parent-child tracking
  - Bus-to-bus forwarding with loop prevention
  - Write-Ahead Logging (WAL) for persistence and replay
  - Retry decorator with exponential backoff and semaphore-based concurrency control
  - Memory management (configurable event history, 50MB warning threshold)
- **Install:** `pip install bubus`

### User Interfaces

#### [`web-ui`](https://github.com/browser-use/web-ui) -- Gradio Web Interface
- **What:** A Gradio-based web UI that wraps browser-use into a user-friendly interface. Run AI agents from your browser.
- **Language:** Python | **Stars:** 15,600+ | **License:** MIT
- **Features:** Multi-LLM support, persistent browser sessions, custom browser integration (use your own Chrome with saved logins), HD screen recording, Docker support.
- **Run:** `python webui.py --ip 127.0.0.1 --port 7788`

#### [`desktop`](https://github.com/browser-use/desktop) -- Desktop Application
- **What:** An Electron + Gradio desktop app that controls your local Chrome browser with AI agents.
- **Language:** TypeScript | **Stars:** 345
- **Features:** Uses your existing Chrome installation (preserves logins/sessions), multi-LLM support, HD screen recording.
- **Status:** Development builds available. Official macOS/Windows/Linux packages coming soon.

#### [`agent-studio`](https://github.com/browser-use/agent-studio) -- Cloud API Demo App
- **What:** A reference implementation / demo for integrating Browser Use Cloud API into web applications.
- **Language:** TypeScript (Next.js) | **Stars:** 24 | **License:** MIT
- **Features:** Live automation viewing via embedded browser iframe, step-by-step progress monitoring with screenshots, file download (PDF, Excel, JSON, ZIP).

### Automation & Workflows

#### [`workflow-use`](https://github.com/browser-use/workflow-use) -- RPA 2.0
- **What:** Create deterministic, self-healing automation workflows. "Show computer what it needs to do once, and it will do it over and over again."
- **Language:** Python | **Stars:** 3,900 | **License:** AGPL-3.0
- **How it works:**
  1. Describe a task or record a browser interaction via the Chrome extension.
  2. The system executes it with browser-use, then generates a semantic workflow with extracted variables.
  3. The workflow is stored and can be replayed with different inputs -- no LLM calls needed for replay.
- **Includes:** Chrome extension for recording, CLI (`python cli.py`), GUI (FastAPI + frontend), programmatic Python API.
- **Status:** Early development. Not recommended for production yet.

#### [`macOS-use`](https://github.com/browser-use/macOS-use) -- macOS App Automation
- **What:** AI agent framework for controlling any macOS application. "Tell your MacBook what to do, and it's done -- across ANY app."
- **Language:** Python | **Stars:** 1,820 | **License:** MIT
- **Install:** `pip install mlx-use`
- **LLM support:** OpenAI, Anthropic, Gemini, DeepSeek R1.
- **Vision:** Local inference via Apple MLX, future expansion to iPhone/iPad.
- **Status:** Under active development. Unsupervised operation not recommended yet.

#### [`contact-use`](https://github.com/browser-use/contact-use) -- Contact Finder
- **What:** Uses browser-use to autonomously find the best way to contact any person or organization.
- **LLM support:** Claude Sonnet or GPT-4.1 Mini (auto-selected based on available API key).
- **Run:** Local server at localhost:8000 with a web interface.

### Testing & QA

#### [`qa-use`](https://github.com/browser-use/qa-use) -- AI-Powered E2E Testing Platform
- **What:** Write end-to-end tests in plain English. AI agents execute them automatically.
- **Language:** TypeScript (Next.js 15) | **Stars:** 481
- **Features:** Test suite management with parallel execution, scheduled runs (hourly/daily), smart email notifications on failure, screenshots and video recording, detailed pass/fail analysis.
- **Stack:** Next.js 15, TypeScript, Docker, PostgreSQL, Inngest (job scheduling), Resend (email).
- **Run:** Docker Compose, accessible at localhost:3000.

#### [`vibetest-use`](https://github.com/browser-use/vibetest-use) -- MCP QA Testing Server
- **What:** An MCP (Model Context Protocol) server that launches multiple browser-use agents to test websites for UI bugs, broken links, accessibility issues, and other problems.
- **Language:** Python | **Stars:** 767
- **Integration:** Works with Claude Code and Cursor as an MCP server.
- **Config:** Specify target URL, number of agents (default 3), headless/visible mode.
- **LLM:** Uses Google Gemini 2.0 Flash.

### Evaluation & Benchmarking

#### [`eval`](https://github.com/browser-use/eval) -- WebVoyager Evaluation
- **What:** Evaluates browser-use against the WebVoyager dataset of web browsing tasks.
- **Language:** Jupyter Notebook | **Stars:** 42 | **License:** Apache-2.0
- **Cost:** ~$250 per full dataset run with GPT-4o.
- **Notes:** Uses LLM-based assessment with manual review for uncertain cases. Tests planning and reasoning capabilities.

#### [`stress-tests`](https://github.com/browser-use/stress-tests) -- Form Autofill Stress Tests
- **What:** 27 different web form implementations to test browser-use's autofill capabilities.
- **Language:** HTML | **Stars:** 22
- **Covers:** Vanilla HTML, jQuery, AngularJS, Angular 2+, React (Hook Form, Formik, Final Form, TanStack), Svelte, Vue, Ember, Material UI, Shadow DOM, Web Components, iframes, contenteditable fields, rich text editors, honeypot fields, non-Latin characters.
- **Validation:** Successful form fill displays "the secret is: dumbledore".

#### [`evaluation-endpoint`](https://github.com/browser-use/evaluation-endpoint)
- **What:** Connects external apps to internal evaluations while keeping sensitive data private.
- **Stars:** 0 | Minimal documentation.

### Community & Documentation

#### [`awesome-prompts`](https://github.com/browser-use/awesome-prompts) -- Prompt Templates
- **What:** Curated collection of browser-use prompt templates across 10 categories.
- **Stars:** 894
- **Categories:** Web Research, E-commerce, Content Creation, Data Extraction, Job Applications, Social Media, Productivity, Form Filling, Testing & QA, Multi-Step Workflows.
- **Includes:** Best practices for prompt structure, clarity, adaptability, and efficiency.

#### [`awesome-projects`](https://github.com/browser-use/awesome-projects) -- Community Projects
- **What:** Curated list of open-source projects built on browser-use.
- **Stars:** 116
- **Notable projects:** OpenManus (multi-agent system), Agent-tars (multimodal web agent), nanobrowser (local multi-agent), SDET-GENIE (QA automation), SpiderCreator (web scraping), Rebrowse (screen recording to workflows).

#### [`docs`](https://github.com/browser-use/docs) -- Documentation (Archived)
- **What:** Previous documentation source. Now archived; docs live at [docs.browser-use.com](https://docs.browser-use.com).
- **Language:** MDX | **Stars:** 8

### Other

#### [`.github`](https://github.com/browser-use/.github) -- Org Profile
- **What:** GitHub organization profile and default community health files.

#### [`vc-use`](https://github.com/browser-use/vc-use)
- **What:** Minimal/empty repository. No description or documentation available.
- **Stars:** 4

---

## How the Repositories Relate

```
                        browser-use (core framework)
                       /       |        \         \
                      /        |         \         \
                cdp-use      bubus     Playwright    LLMs
              (CDP types)  (event bus)  (browser)   (brain)
                    \         |          /
                     \        |         /
                      browser-use Agent
                     /    |     \      \
                    /     |      \      \
              web-ui  desktop  workflow-use  macOS-use
              (Gradio) (Electron) (RPA 2.0)  (macOS apps)
                 |
            agent-studio
            (Cloud demo)

        Testing & QA                    Community
        ─────────────                   ─────────
        qa-use (E2E platform)           awesome-prompts
        vibetest-use (MCP QA)           awesome-projects
        eval (WebVoyager benchmark)     docs (archived)
        stress-tests (form tests)
        evaluation-endpoint

        Utilities
        ─────────
        contact-use (contact finder)
```

### Dependency Chain

1. **cdp-use** generates type-safe Python bindings for the Chrome DevTools Protocol.
2. **bubus** provides the event bus for async event-driven communication within browser-use.
3. **browser-use** is the core framework that combines CDP communication, event handling, LLM integration, and the agent loop.
4. **web-ui**, **desktop**, **workflow-use**, **macOS-use**, **qa-use**, **vibetest-use**, **contact-use**, and **agent-studio** are all built on top of browser-use.

---

## Technical Specifications

| Spec | Value |
|---|---|
| **Primary language** | Python (>=3.11) |
| **Browser engine** | Chromium via Playwright + CDP |
| **Async runtime** | asyncio |
| **Data validation** | Pydantic v2 |
| **LLM abstraction** | LangChain |
| **Package manager** | uv (recommended), pip |
| **License** | MIT (most repos), AGPL-3.0 (workflow-use) |
| **Cloud platform** | Browser Use Cloud (cloud.browser-use.com) |
| **Documentation** | docs.browser-use.com |
| **PyPI package** | `browser-use` (v0.11.11 as of Feb 2026) |
| **CLI commands** | `browser-use`, `browseruse`, `bu`, `browser` |

---

## Key Concepts for Agents

1. **browser-use is not a browser.** It is an AI agent framework that *controls* a browser. The browser is Chromium; the brain is an LLM; browser-use is the orchestrator.

2. **The agent loop is the core abstraction.** Every task follows the same cycle: observe browser state -> LLM decides action -> execute action -> repeat.

3. **DOM extraction is how the agent "sees."** The page DOM is processed into a simplified representation of interactive elements (buttons, links, inputs, etc.) that the LLM can reason about.

4. **Custom tools extend the action space.** Developers add domain-specific capabilities (e.g., database lookups, API calls) that the agent can invoke alongside browser actions.

5. **Cloud vs local is a deployment choice.** Local is free and good for development. Cloud adds stealth, proxies, parallelism, and scalability for production.

6. **workflow-use captures workflows as replayable recordings.** Instead of prompting the AI every time, record once and replay deterministically -- no LLM needed for replay.

7. **bubus is the internal nervous system.** Events flow through the bus for handler coordination, result aggregation, and persistence.

8. **The ecosystem is modular.** Each repo solves one problem. Combine them as needed: browser-use + web-ui for a GUI, browser-use + workflow-use for RPA, browser-use + qa-use for testing.
