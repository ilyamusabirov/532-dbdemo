# RAG Demo

Marimo notebook demonstrating Retrieval-Augmented Generation (RAG) with a taxi dataset knowledge base.

## What it shows

1. **Knowledge base** — plain text paragraphs about NYC taxi data fields (`knowledge_base/`)
2. **TF-IDF retrieval** — find relevant chunks using cosine similarity (no API key needed)
3. **LLM comparison** — side-by-side: response without RAG vs. with injected context
4. **Shiny wiring** — how to plug this pattern into a `querychat` app

## Run

```bash
# Install deps (from repo root)
pip install -r requirements.txt

# Set env var
export ANTHROPIC_API_KEY=...

# Editable notebook
marimo edit rag/rag_demo.py

# Read-only app mode
marimo run rag/rag_demo.py
```

## Env vars needed

| Variable | Purpose |
|---|---|
| `ANTHROPIC_API_KEY` | LLM calls (claude-haiku-4-5) |
