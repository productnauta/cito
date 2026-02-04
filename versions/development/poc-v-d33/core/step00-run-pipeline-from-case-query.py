#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step00-run-pipeline-from-case-query.py
Version: poc-v-d33      Date: 2026-02-02
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Executa o fluxo completo a partir de TODOS os itens em case_query, ignorando status.
Inputs: config/mongo.yaml, config/pipeline.yaml, case_query.htmlRaw.
Outputs: case_data atualizado + execução dos steps 02-09 por stfDecisionId extraído.
Dependencies: pymongo, beautifulsoup4 (via step01), subprocess
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import importlib.machinery
import importlib.util
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import yaml

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
PIPELINE_CONFIG_PATH = CONFIG_DIR / "pipeline.yaml"
STEP01_PATH = BASE_DIR / "step01-extract-cases.py"


def _ts() -> str:
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _load_step01_module():
    loader = importlib.machinery.SourceFileLoader("step01_extract_cases", str(STEP01_PATH))
    spec = importlib.util.spec_from_loader(loader.name, loader)
    if spec is None or spec.loader is None:
        raise RuntimeError("Falha ao carregar step01-extract-cases.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _run_step(script: str, input_text: str) -> int:
    cmd = [sys.executable, str(BASE_DIR / script)]
    result = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        cwd=str(BASE_DIR),
    )
    return result.returncode


def _iter_case_query_docs(case_query_col) -> Iterable[Dict[str, Any]]:
    return case_query_col.find({}, projection={"htmlRaw": 1, "queryString": 1, "pageSize": 1, "inteiroTeor": 1})


def _collect_stf_ids(docs: List[Dict[str, Any]]) -> List[str]:
    ids: List[str] = []
    seen = set()
    for doc in docs:
        identity = doc.get("identity") if isinstance(doc.get("identity"), dict) else {}
        stf_id = (identity.get("stfDecisionId") or "").strip()
        if not stf_id or stf_id in seen:
            continue
        seen.add(stf_id)
        ids.append(stf_id)
    return ids


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Executa pipeline a partir de todos os itens de case_query (ignora status)."
    )
    parser.add_argument(
        "--case-url",
        help="caseContent.caseUrl override (usado no step02 se necessario).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limita a quantidade de documentos de case_query processados.",
    )
    args = parser.parse_args()

    case_url_override = (args.case_url or "").strip()

    log("Carregando step01-extract-cases.py")
    step01 = _load_step01_module()

    log("Carregando config pipeline.yaml")
    pipeline_raw = _load_yaml(PIPELINE_CONFIG_PATH)
    pipeline_cfg = pipeline_raw.get("pipeline") if isinstance(pipeline_raw.get("pipeline"), dict) else {}

    execution_cfg = pipeline_cfg.get("execution") if isinstance(pipeline_cfg.get("execution"), dict) else {}
    runtime_cfg = pipeline_cfg.get("runtime") if isinstance(pipeline_cfg.get("runtime"), dict) else {}

    delay_steps = float(execution_cfg.get("delay_between_steps_seconds") or 2.0)
    delay_items = float(execution_cfg.get("delay_between_items_seconds") or 10.0)

    steps_cfg = execution_cfg.get("steps") if isinstance(execution_cfg.get("steps"), list) else []
    step_defs: List[tuple[str, str]] = []
    for s in steps_cfg:
        if not isinstance(s, dict):
            continue
        if s.get("enabled") is False:
            continue
        script = str(s.get("script") or "").strip()
        if not script:
            continue
        if script == "step01-extract-cases.py":
            continue
        fmt = str(s.get("input_format") or "{stfDecisionId}\n")
        step_defs.append((script, fmt))

    case_url = case_url_override or str(runtime_cfg.get("case_url_override") or "").strip()

    log("Carregando config mongo.yaml")
    mongo_raw = step01.load_yaml(MONGO_CONFIG_PATH)
    mongo_cfg = step01.build_mongo_cfg(mongo_raw)
    log(f"MongoDB config OK | db='{mongo_cfg.database}'")

    log("Conectando ao MongoDB e obtendo collections")
    case_query_col, case_col = step01.get_collections(mongo_cfg)

    docs_cursor = _iter_case_query_docs(case_query_col)
    docs = list(docs_cursor)
    if args.limit and args.limit > 0:
        docs = docs[: args.limit]

    if not docs:
        log("Nenhum documento encontrado em case_query.")
        return 0

    log(f"Total de case_query encontrados: {len(docs)}")

    processed_query = 0
    for idx, case_query_doc in enumerate(docs, start=1):
        doc_id = case_query_doc.get("_id")
        doc_id_str = str(doc_id)
        log(f"({idx}/{len(docs)}) Processando case_query._id={doc_id_str}")

        html_raw = (case_query_doc.get("htmlRaw") or "").strip()
        if not html_raw:
            log("ERRO: htmlRaw vazio. Ignorando este case_query.")
            continue

        query_sub = step01.build_query_from_case_query(case_query_doc)
        if query_sub:
            log(f"Query detectada | {query_sub}")

        extracted_docs = step01.extract_cards(html_raw, doc_id_str)
        if query_sub:
            for d in extracted_docs:
                step01._set_if(d, "query", query_sub)

        inserted = 0
        updated = 0
        skipped = 0
        for i, d in enumerate(extracted_docs, start=1):
            identity = d.get("identity") if isinstance(d.get("identity"), dict) else {}
            stf_id = step01._clean_str(identity.get("stfDecisionId"))
            if not stf_id:
                skipped += 1
                log(f"WARN: doc #{i} sem stfDecisionId (ignorado)")
                continue

            created, saved_doc_id = step01.upsert_case_data(case_col, doc=d, stf_decision_id=stf_id)
            if created:
                inserted += 1
            else:
                updated += 1

        log(f"Persistencia case_data | inseridos={inserted} | atualizados={updated} | ignorados={skipped}")

        case_query_col.update_one(
            {"_id": doc_id},
            {"$set": {"status": mongo_cfg.status_ok, "processedDate": step01.utc_now(), "extractedCount": len(extracted_docs)}},
        )

        stf_ids = _collect_stf_ids(extracted_docs)
        if not stf_ids:
            log("Nenhum stfDecisionId extraido para este case_query.")
            processed_query += 1
            continue

        log(f"Total de stfDecisionId extraidos: {len(stf_ids)}")

        for j, stf_id in enumerate(stf_ids, start=1):
            log(f"  ({j}/{len(stf_ids)}) Executando steps para stfDecisionId={stf_id}")
            for script, fmt in step_defs:
                input_text = fmt.format(stfDecisionId=stf_id, caseUrl=case_url or "")
                log(f"  Executando: {script}")
                rc = _run_step(script, input_text)
                if rc != 0:
                    log(f"  ERRO: {script} retornou codigo {rc}")
                else:
                    log(f"  OK: {script}")
                if delay_steps > 0:
                    time.sleep(delay_steps)
            if j < len(stf_ids) and delay_items > 0:
                log(f"  Aguardando {delay_items}s antes do proximo stfDecisionId...")
                time.sleep(delay_items)

        processed_query += 1

    log(f"Pipeline concluida | case_query processados={processed_query}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
