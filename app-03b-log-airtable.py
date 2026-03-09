"""querychat query logger — Airtable backend (app-03b).

Logs every generated SQL query + user question + row count to Airtable.
Works on Posit Connect: atomic writes, persistent across restarts, safe under
multiple workers.

Compare with app-03a-log-local.py — only save_info() and load_data() differ.

Run: shiny run app-03b-log-airtable.py
Requires in .env:
  GITHUB_TOKEN      (or other LLM key)
  AIRTABLE_API_KEY  — airtable.com/create/tokens
  AIRTABLE_BASE_ID  — from your base URL: airtable.com/appXXX/tblXXX
  AIRTABLE_TABLE_ID — from your base URL
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
import querychat
from chatlas import ChatGithub
from dotenv import load_dotenv
from pyairtable import Api
from seaborn import load_dataset
from shiny import App, reactive, render, ui

load_dotenv(Path(__file__).parent / ".env")

# ── Data + querychat setup ────────────────────────────────────────────────────

titanic = load_dataset("titanic")
qc = querychat.QueryChat(
    titanic,
    "titanic",
    client=ChatGithub(model="gpt-4.1-mini"),
)

# ── Persistent storage: Airtable ──────────────────────────────────────────────

_api = Api(os.environ["AIRTABLE_API_KEY"])
table = _api.table(os.environ["AIRTABLE_BASE_ID"], os.environ["AIRTABLE_TABLE_ID"])

SCHEMA = ["timestamp", "user_query", "sql", "n_rows"]


def save_info(row: dict) -> None:
    table.create(row)              # atomic HTTP write — safe under multiple workers


def load_data() -> pd.DataFrame:
    records = table.all()
    rows = [r["fields"] for r in records]
    return pd.DataFrame(rows, columns=SCHEMA) if rows else pd.DataFrame(columns=SCHEMA)


# ── UI ────────────────────────────────────────────────────────────────────────

app_ui = ui.page_sidebar(
    qc.sidebar(),
    ui.card(
        ui.card_header(ui.output_text("title")),
        ui.output_data_frame("data_table"),
        fill=True,
    ),
    ui.card(
        ui.card_header("Query Log (Airtable)"),
        ui.output_data_frame("log_table"),
    ),
    fillable=True,
    title="Titanic Explorer — with Airtable Logging",
)

# ── Server ────────────────────────────────────────────────────────────────────


def server(input, output, session):
    qc_vals = qc.server()

    log = reactive.value(load_data())   # in-memory view, pre-loaded from Airtable

    @reactive.effect
    def log_query():
        sql = qc_vals.sql()
        if not sql:           # None or empty string — no query generated yet
            return

        turns = qc_vals.client.get_turns()
        user_turns = [t for t in turns if t.role == "user"]
        user_query = user_turns[-1].text if user_turns else "(unknown)"

        row = {
            "timestamp": datetime.now().isoformat(timespec="seconds"),
            "user_query": user_query,
            "sql": sql,
            "n_rows": str(len(qc_vals.df())),   # Airtable text field — cast to str
        }

        save_info(row)
        log.set(pd.concat([log(), pd.DataFrame([row])], ignore_index=True))

    @render.text
    def title():
        return qc_vals.title() or "Titanic dataset"

    @render.data_frame
    def data_table():
        return qc_vals.df()

    @render.data_frame
    def log_table():
        return render.DataGrid(log(), width="100%")


app = App(app_ui, server)
