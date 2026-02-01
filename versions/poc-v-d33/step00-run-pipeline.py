#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step00-run-pipeline.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Orchestrates the pipeline for case_data documents missing processing.caseScrapeStatus.
Inputs: Mongo config (config/mongo.json), case_data records, optional case URL override.
Outputs: Executes steps 02-08, logs progress, and relies on each step to persist results.
Pipeline: get case HTML -> clean HTML -> extract sections -> parties/keywords -> legislation -> notes -> doctrine.
Dependencies: pymongo
------------------------------------------------------------

"""

from __future__ import annotations

import argparse
import json
import time
import subprocess
import sys
from pathlib import Path
from typing import Any, Dict, List

from pymongo import MongoClient


BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
# Delay entre execuções de stfDecisionId (em segundos).
DELAY_BETWEEN_ITEMS_SECONDS = 10.0


def _load_json(path: Path) -> Dict[str, Any]:
    # Carrega JSON do disco com validação mínima.
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def _get_case_data_collection():
    # Conecta ao MongoDB usando config/mongo.json e retorna a collection case_data.
    raw = _load_json(MONGO_CONFIG_PATH)
    mongo = raw.get("mongo")
    if not isinstance(mongo, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida.")
    uri = str(mongo.get("uri") or "").strip()
    database = str(mongo.get("database") or "").strip()
    if not uri or not database:
        raise ValueError("Config inválida: 'mongo.uri' ou 'mongo.database' vazio.")
    client = MongoClient(uri)
    return client[database]["case_data"]


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
    print("[INFO] Conectando ao MongoDB...")
    try:
        col = _get_case_data_collection()
    except Exception as e:
        print(f"[ERRO] Falha ao conectar no MongoDB: {e}")
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

    print("[INFO] Buscando documentos sem processing.caseScrapeStatus...")
    docs = list(col.find(query, projection=projection))
    if not docs:
        print("[INFO] Nenhum documento encontrado para processar.")
        return 0

    # 3) Para cada documento encontrado, executar os steps em sequência
    total = len(docs)
    print(f"[INFO] Total encontrado: {total}")

    for idx, doc in enumerate(docs, start=1):
        stf_decision_id = ((doc.get("identity") or {}).get("stfDecisionId") or "").strip()
        if not stf_decision_id:
            print(f"[WARN] Registro sem identity.stfDecisionId (pos {idx}/{total}). Ignorando.")
            continue

        print(f"\n[INFO] ({idx}/{total}) Processando stfDecisionId={stf_decision_id}")

        steps = [
            ("step02-get-case-html.py", f"{stf_decision_id}\n{case_url}\n" if case_url else f"{stf_decision_id}\n"),
            ("step03-clean-case-html.py", f"2\n{stf_decision_id}\n"),
            ("step04-extract-sessions.py", f"2\n{stf_decision_id}\n"),
            ("step05-extract-keywords-parties.py", f"{stf_decision_id}\n"),
            ("step06-extract-legislation-groq.py", f"{stf_decision_id}\n"),
            ("step07-extract-notes-groq.py", f"{stf_decision_id}\n"),
            ("step08-doctrine-legislation-ai.py", f"{stf_decision_id}\n"),
        ]

        for script, input_text in steps:
            print(f"[INFO] Executando: {script}")
            rc = _run_step(script, input_text)
            if rc != 0:
                print(f"[ERRO] {script} retornou código {rc}")
            else:
                print(f"[OK] {script}")
        # Mesmo com falhas em steps individuais, seguimos com o próximo step e depois com o próximo registro.

        # Aguarda antes de iniciar o próximo item (rate limit simples).
        if idx < total and DELAY_BETWEEN_ITEMS_SECONDS > 0:
            print(f"[INFO] Aguardando {DELAY_BETWEEN_ITEMS_SECONDS}s antes do próximo item...")
            time.sleep(DELAY_BETWEEN_ITEMS_SECONDS)

    print("[INFO] Pipeline finalizado.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
