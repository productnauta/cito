#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step03-clean-case-html.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Cleans the raw case HTML and extracts the main content container.
Inputs: config/mongo.json, case_data.caseContent.caseHtml.
Outputs: case_data.caseContent.caseHtmlClean + processing/status updates.
Pipeline: load HTML -> extract main div -> persist clean HTML + metadata.
Dependencies: pymongo beautifulsoup4
------------------------------------------------------------

"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# =============================================================================
# 0) LOG / TIME
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


# =============================================================================
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"

CASE_DATA_COLLECTION = "case_data"


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config invalida: chave 'mongo' ausente ou invalida.")

    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri:
        raise ValueError("Config invalida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config invalida: 'mongo.database' vazio.")

    return MongoCfg(uri=uri, database=db)


def get_case_data_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("STEP", "Validando conexao (ping)")
    client.admin.command("ping")

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) EXTRACAO DO CONTEUDO PRINCIPAL
# =============================================================================

TARGET_CSS = "#mat-tab-content-0-0 > div > div"


def extract_case_html(case_html: str) -> Tuple[str, Dict[str, Any]]:
    """
    Extrai apenas o conteudo da div principal do processo.
    Retorna (html_clean, meta).
    """
    meta: Dict[str, Any] = {
        "method": None,
        "selector": None,
    }

    if not case_html or not case_html.strip():
        return "", meta

    soup = BeautifulSoup(case_html, "html.parser")

    # Metodo 1: CSS selector
    target = soup.select_one(TARGET_CSS)
    if target is not None:
        meta["method"] = "css_selector"
        meta["selector"] = TARGET_CSS
        return target.decode_contents().strip(), meta

    # Metodo 2: id + filhos div/div
    container = soup.find(id="mat-tab-content-0-0")
    if container is not None:
        div1 = container.find("div")
        if div1 is not None:
            div2 = div1.find("div")
            if div2 is not None:
                meta["method"] = "id_div_div"
                meta["selector"] = "#mat-tab-content-0-0 > div > div"
                return div2.decode_contents().strip(), meta

    return "", meta


# =============================================================================
# 3) UPDATE HELPERS
# =============================================================================

def persist_success(col: Collection, doc_id: Any, *, clean_html: str, meta: Dict[str, Any]) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "caseContent.caseHtmlClean": clean_html,
            "processing.caseHtmlCleanedAt": utc_now(),
            "processing.caseHtmlCleanMeta": meta,
            "processing.caseHtmlCleanError": None,
            "processing.pipelineStatus": "caseHtmlCleaned",
            "audit.updatedAt": utc_now(),
            "audit.lastCaseHtmlCleanedAt": utc_now(),
            "status.pipelineStatus": "caseHtmlCleaned",
        }},
    )


def persist_error(col: Collection, doc_id: Any, *, err: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "processing.caseHtmlCleanedAt": utc_now(),
            "processing.caseHtmlCleanError": err,
            "processing.pipelineStatus": "caseHtmlCleanError",
            "audit.updatedAt": utc_now(),
        }},
    )


# =============================================================================
# 4) MAIN
# =============================================================================

def _prompt_mode() -> Tuple[str, Optional[str]]:
    """
    Pergunta ao usuario o modo de execucao.
    Retorna (mode, case_stf_id).
    mode in {"all", "one"}
    """
    print("\nSelecione o modo de execucao:")
    print("  1) Processar TODOS com status.pipelineStatus = caseScraped")
    print("  2) Processar APENAS um documento por identity.stfDecisionId")

    while True:
        choice = input("Opcao (1/2): ").strip()
        if choice == "1":
            return "all", None
        if choice == "2":
            case_stf_id = input("Informe o identity.stfDecisionId: ").strip()
            if case_stf_id:
                return "one", case_stf_id
            print("identity.stfDecisionId vazio. Tente novamente.")
        else:
            print("Opcao invalida. Use 1 ou 2.")


def _log_doc_header(doc: Dict[str, Any]) -> None:
    doc_id = doc.get("_id")
    case_stf_id = (doc.get("identity") or {}).get("stfDecisionId")
    title = doc.get("caseTitle") or "N/A"
    status = (doc.get("status", {}) or {}).get("pipelineStatus")
    log("INFO", f"Documento | _id={doc_id} | identity.stfDecisionId={case_stf_id} | status={status} | title='{title}'")


def process_document(col: Collection, doc: Dict[str, Any]) -> bool:
    doc_id = doc.get("_id")

    try:
        col.update_one(
            {"_id": doc_id},
            {"$set": {"processing.caseHtmlCleaningAt": utc_now()}},
        )

        case_html = ((doc.get("caseContent") or {}).get("caseHtml") or "").strip()
        if not case_html:
            raise ValueError("Campo caseContent.caseHtml vazio.")

        log("STEP", "Extraindo conteudo principal do HTML")
        clean_html, meta = extract_case_html(case_html)
        if not clean_html:
            raise RuntimeError("Falha ao localizar a div principal (conteudo vazio).")

        log("OK", f"Conteudo extraido | metodo={meta.get('method')} | selector={meta.get('selector')}")
        log("STEP", "Persistindo caseContent.caseHtmlClean + status")
        persist_success(col, doc_id, clean_html=clean_html, meta=meta)
        log("OK", "Documento atualizado com sucesso")
        return True

    except Exception as e:
        log("ERROR", f"Erro ao processar documento | _id={doc_id} | erro={e}")
        persist_error(col, doc_id, err=str(e))
        return False


def main() -> int:
    log("INFO", "Iniciando etapa: SANITIZAR HTML DO PROCESSO (caseHtml -> caseHtmlClean)")

    try:
        col = get_case_data_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    mode, case_stf_id = _prompt_mode()

    if mode == "all":
        base_filter: Dict[str, Any] = {"status.pipelineStatus": "caseScraped"}
        log("STEP", "Buscando documentos com status.pipelineStatus = caseScraped")
        try:
            cursor = col.find(base_filter, projection={"caseContent.caseHtml": 1, "identity.stfDecisionId": 1, "caseTitle": 1, "status.pipelineStatus": 1})
        except PyMongoError as e:
            log("ERROR", f"Erro ao consultar documentos: {e}")
            return 1

        total = 0
        ok = 0
        for doc in cursor:
            total += 1
            _log_doc_header(doc)
            if process_document(col, doc):
                ok += 1

        log("INFO", f"Finalizado | total={total} | ok={ok} | erro={total - ok}")
        return 0 if total == ok else 1

    # mode == "one"
    log("STEP", f"Buscando documento por identity.stfDecisionId='{case_stf_id}'")
    try:
        doc = col.find_one(
            {"identity.stfDecisionId": case_stf_id},
            projection={"caseContent.caseHtml": 1, "identity.stfDecisionId": 1, "caseTitle": 1, "status.pipelineStatus": 1},
        )
    except PyMongoError as e:
        log("ERROR", f"Erro ao consultar documento: {e}")
        return 1

    if not doc:
        log("WARN", f"Nenhum documento encontrado para identity.stfDecisionId='{case_stf_id}'")
        return 1

    _log_doc_header(doc)
    ok = process_document(col, doc)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
