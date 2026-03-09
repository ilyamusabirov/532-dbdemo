"""app-03-form.py — Simple feedback form → MongoDB Atlas.

Collects name + comment from user and writes to MongoDB on submit.
Also includes an editable DataGrid for batch inserts.

Run: shiny run app-03-form.py
Requires in .env:
  MONGODB_URI    — from Atlas: mongodb+srv://user:pass@cluster.mongodb.net/
"""

import os
from datetime import datetime
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient
from shiny import App, reactive, render, ui

load_dotenv(Path(__file__).parent / ".env")

# ── MongoDB setup ─────────────────────────────────────────────
_client = MongoClient(os.environ["MONGODB_URI"])
collection = _client["dsci532"]["feedback_form"]

SCHEMA = ["name", "comment", "timestamp"]
INPUT_COLUMNS = ["name", "comment"]

def save_info(row: dict) -> None:
    collection.insert_one(row)

def load_data() -> pd.DataFrame:
    rows = list(collection.find({}, {"_id": 0}))
    return pd.DataFrame(rows, columns=SCHEMA) if rows else pd.DataFrame(columns=SCHEMA)

# ── UI ────────────────────────────────────────────────────────
app_ui = ui.page_sidebar(
    ui.sidebar(
        ui.input_text("name", "Your name"),
        ui.input_text_area("comment", "Comment"),
        ui.input_action_button("submit", "Submit"),
        ui.hr(),
        ui.h4("Batch input: Editable DataGrid"),
        ui.output_data_frame("response_table"),
        ui.input_action_button("submit_grid", "Submit grid edits"),
    ),
    ui.h3("Feedback log"),
    ui.output_data_frame("log_table"),
)

# ── Server ───────────────────────────────────────────────────
def server(input, output, session):
    log_data = reactive.value(load_data())

    def build_entry(name: str, comment: str) -> dict:
        return {
            "name": name,
            "comment": comment,
            "timestamp": datetime.now().isoformat(timespec="seconds"),
        }

    @reactive.effect
    @reactive.event(input.submit)
    def save_response():
        name = input.name().strip()
        comment = input.comment().strip()
        if not (name or comment):
            return
        save_info(build_entry(name, comment))
        log_data.set(load_data())

    # Editable DataGrid
    template_df = pd.DataFrame([{c: "" for c in INPUT_COLUMNS} for _ in range(3)], columns=INPUT_COLUMNS)

    @render.data_frame
    def response_table():
        return render.DataGrid(template_df, editable=True)

    @reactive.effect
    @reactive.event(input.submit_grid)
    def save_grid():
        df = response_table.data_view()
        if df.empty:
            return

        df = df.fillna("")
        has_content = (
            df["name"].astype(str).str.strip().ne("")
            | df["comment"].astype(str).str.strip().ne("")
        )
        for _, row in df.loc[has_content, INPUT_COLUMNS].iterrows():
            save_info(build_entry(str(row["name"]).strip(), str(row["comment"]).strip()))
        if has_content.any():
            log_data.set(load_data())

    @render.data_frame
    def log_table():
        return render.DataGrid(log_data(), width="100%")

app = App(app_ui, server)
