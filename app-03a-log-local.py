"""querychat query logger — local CSV backend (app-03a).

Logs every generated SQL query + user question + row count to a local CSV file.

Works locally. Breaks on Posit Connect:
  - Container restarts wipe the file (ephemeral filesystem)
  - Multiple workers cause race conditions (concurrent CSV writes)

For the Airtable version that works on Posit Connect: see app-03b-log-airtable.py

Run: shiny run app-03a-log-local.py
Requires: OPENAI_API_KEY (or other LLM key) in .env
"""

from datetime import datetime
from pathlib import Path

import pandas as pd
import querychat
from chatlas import ChatGithub
from dotenv import load_dotenv
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

# ── Persistent storage: local CSV ─────────────────────────────────────────────
# ⚠️  Works locally — NOT suitable for Posit Connect (see module docstring)

LOG_FILE = Path(__file__).parent / "logs" / "query_log.csv"
SCHEMA = ["timestamp", "user_query", "sql", "n_rows"]


def save_info(row: dict) -> None:
    df = pd.DataFrame([row])
    LOG_FILE.parent.mkdir(exist_ok=True)
    df.to_csv(LOG_FILE, mode="a", header=not LOG_FILE.exists(), index=False)


def load_data() -> pd.DataFrame:
    if LOG_FILE.exists():
        return pd.read_csv(LOG_FILE)
    return pd.DataFrame(columns=SCHEMA)


# ── UI ────────────────────────────────────────────────────────────────────────

app_ui = ui.page_sidebar(
    qc.sidebar(),
    ui.card(
        ui.card_header(ui.output_text("title")),
        ui.output_data_frame("data_table"),
        fill=True,
    ),
    ui.card(
        ui.card_header("Query Log (local CSV)"),
        ui.output_data_frame("log_table"),
    ),
    fillable=True,
    title="Titanic Explorer — with Query Logging",
)

# ── Server ────────────────────────────────────────────────────────────────────


def server(input, output, session):
    qc_vals = qc.server()

    log = reactive.value(load_data())
    pending = reactive.value(None)    # bridge: hook sets, effect reads

    def on_query(req):
        """Fires inside Extended Task — only .set() is allowed here, no reactive reads."""
        if req.name not in ("querychat_update_dashboard", "querychat_query"):
            return
        sql = req.arguments.get("query", "")
        if not sql:
            return
        turns = qc_vals.client.get_turns()
        user_turns = [t for t in turns if t.role == "user"]
        pending.set({                       # .set() is safe from Extended Task
            "user_query": user_turns[-1].text if user_turns else "(unknown)",
            "sql": sql,
            "tool": req.name,
        })

    qc_vals.client.on_tool_request(on_query)

    @reactive.effect
    def flush_log():
        entry = pending()
        if not entry:
            return
        entry["n_rows"] = len(qc_vals.df())    # reactive read — safe here
        entry["timestamp"] = datetime.now().isoformat(timespec="seconds")
        save_info(entry)
        log.set(pd.concat(
            [log(), pd.DataFrame([entry])], ignore_index=True
        ))
        pending.set(None)                       # clear to avoid re-fire
        log.set(pd.concat(
            [log(), pd.DataFrame([row])], ignore_index=True
        ))

    qc_vals.client.on_tool_request(on_query)

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
