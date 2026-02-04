#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step00-run-pipeline.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Orchestrates the pipeline for case_data documents missing processing.caseScrapeStatus.
Inputs: Mongo config (config/mongo.yaml), case_data records, optional case URL override.
Outputs: Executes steps 02-08, logs progress, and relies on each step to persist results.
Pipeline: get case HTML -> clean HTML -> extract sections -> parties/keywords -> legislation -> notes -> doctrine.
Dependencies: pymongo
------------------------------------------------------------

"""

from __future__ import annotations

import argparse
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

from utils.mongo import get_case_data_collection


BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
# Delay entre execuções de stfDecisionId (em segundos).
DELAY_BETWEEN_ITEMS_SECONDS = 10.0

# Logs
LOG_DIR = BASE_DIR / "logs"
LOG_FILE_PATH: Path | None = None
LOG_TO_FILE = True


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _init_log_file() -> None:
    global LOG_FILE_PATH
    if LOG_FILE_PATH is not None:
        return
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d-%H%M%S")
    LOG_FILE_PATH = LOG_DIR / f"pipeline-{stamp}.log"


def _write_log_file(text: str) -> None:
    if not LOG_TO_FILE:
        return
    if LOG_FILE_PATH is None:
        _init_log_file()
    try:
        with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        # Evita quebrar o pipeline caso falhe escrita em disco
        pass


def log(msg: str) -> None:
    line = f"[{_ts()}] {msg}"
    print(line)
    _write_log_file(line + "\n")


def _get_case_data_collection():
    # Conecta ao MongoDB usando config/mongo.yaml e retorna a collection case_data.
    return get_case_data_collection(MONGO_CONFIG_PATH, "case_data")


def _run_step(script: str, input_text: str) -> int:
    # Executa o step como subprocesso, injetando o input necessário via stdin.
    cmd = [sys.executable, str(BASE_DIR / script)]
    result = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        cwd=str(BASE_DIR),
    )
    return result.returncode


def main() -> int:
    _init_log_file()
    parser = argparse.ArgumentParser(description="Executa o pipeline por stfDecisionId.")
    parser.add_argument(
        "--case-url",
        help="caseContent.caseUrl (use se o step02 não encontrar URL no MongoDB).",
    )
    parser.add_argument(
        "--continue-on-error",
        action="store_true",
        help="Continua executando os próximos steps mesmo após erro.",
    )
    args = parser.parse_args()

    case_url = (args.case_url or "").strip()

    # 1) Conectar no MongoDB e localizar documentos sem processing.caseScrapeStatus
    log("[INFO] Conectando ao MongoDB...")
    try:
        col = _get_case_data_collection()
    except Exception as e:
        log(f"[ERRO] Falha ao conectar no MongoDB: {e}")
        return 1

    # 2) Query: documentos com processing.caseScrapeStatus ausente/nulo/vazio
    query = {
        "$or": [
            {"processing.caseScrapeStatus": {"$exists": False}},
            {"processing.caseScrapeStatus": None},
            {"processing.caseScrapeStatus": ""},
        ]
    }
    projection = {"identity.stfDecisionId": 1}

    log("[INFO] Buscando documentos sem processing.caseScrapeStatus...")
    docs = list(col.find(query, projection=projection))
    if not docs:
        log("[INFO] Nenhum documento encontrado para processar.")
        return 0

    # 3) Para cada documento encontrado, executar os steps em sequência
    total = len(docs)
    log(f"[INFO] Total encontrado: {total}")

    for idx, doc in enumerate(docs, start=1):
        stf_decision_id = ((doc.get("identity") or {}).get("stfDecisionId") or "").strip()
        if not stf_decision_id:
            log(f"[WARN] Registro sem identity.stfDecisionId (pos {idx}/{total}). Ignorando.")
            continue

        log(f"[INFO] ({idx}/{total}) Processando stfDecisionId={stf_decision_id}")

        steps = [
            ("step02-get-case-html.py", f"{stf_decision_id}\n{case_url}\n" if case_url else f"{stf_decision_id}\n"),
            ("step03-clean-case-html.py", f"{stf_decision_id}\n"),
            ("step04-extract-sessions.py", f"{stf_decision_id}\n"),
            ("step05-extract-keywords-parties.py", f"{stf_decision_id}\n"),
            ("step06-extract-legislation-mistral.py", f"{stf_decision_id}\n"),
            ("step07-extract-notes-mistral.py", f"{stf_decision_id}\n"),
            ("step08-doctrine-mistral.py", f"{stf_decision_id}\n"),
        ]

        for script, input_text in steps:
            log(f"[INFO] Executando: {script}")
            rc = _run_step(script, input_text)
            if rc != 0:
                log(f"[ERRO] {script} retornou código {rc}")
            else:
                log(f"[OK] {script}")
        # Mesmo com falhas em steps individuais, seguimos com o próximo step e depois com o próximo registro.

        # Aguarda antes de iniciar o próximo item (rate limit simples).
        if idx < total and DELAY_BETWEEN_ITEMS_SECONDS > 0:
            log(f"[INFO] Aguardando {DELAY_BETWEEN_ITEMS_SECONDS}s antes do próximo item...")
            time.sleep(DELAY_BETWEEN_ITEMS_SECONDS)

    log("[INFO] Pipeline finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
