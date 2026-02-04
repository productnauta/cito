#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
d-scrape-case-html.py

Objetivo:
- Solicitar identity.stfDecisionId
- Obter caseContent.caseUrl do documento (ou solicitar se não existir)
- Baixar o HTML completo da página
- Salvar em caseContent.caseHtml (criando ou atualizando o documento)
- Atualizar processing/audit/status

Dependencias:
  pip install pymongo requests
"""

from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import requests
import certifi
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
# 2) HTTP
# =============================================================================

def fetch_html(url: str, *, timeout: int = 30) -> str:
    """Baixa o HTML completo da pagina."""
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    combined = CONFIG_DIR / "combined_cacert.pem"
    if combined.exists():
        verify = str(combined)
        log("INFO", f"Using combined CA bundle: {verify}")
    else:
        verify = certifi.where()
        log("INFO", f"Using certifi bundle: {verify}")

    resp = requests.get(url, headers=headers, timeout=timeout, verify=verify)
    if resp.status_code != 200:
        raise RuntimeError(f"HTTP {resp.status_code} ao baixar HTML")
    return resp.text


# =============================================================================
# 3) UPDATE HELPERS
# =============================================================================

def persist_success(
    col: Collection,
    stf_decision_id: str,
    *,
    case_url: str,
    case_html: str,
    doc_id: Optional[Any],
) -> None:
    update = {
        "identity.stfDecisionId": stf_decision_id,
        "caseContent.caseUrl": case_url,
        "caseContent.caseHtml": case_html,
        "processing.caseScrapedAt": utc_now(),
        "processing.caseScrapeError": None,
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": "caseScraped",
    }

    # Upsert garante criacao quando nao existe
    col.update_one(
        {"_id": doc_id} if doc_id is not None else {"identity.stfDecisionId": stf_decision_id},
        {"$set": update},
        upsert=doc_id is None,
    )


def persist_error(
    col: Collection,
    stf_decision_id: str,
    *,
    doc_id: Optional[Any],
    err: str,
) -> None:
    update = {
        "processing.caseScrapedAt": utc_now(),
        "processing.caseScrapeError": err,
        "audit.updatedAt": utc_now(),
    }
    # Em erro, nao altera status.pipelineStatus
    col.update_one(
        {"_id": doc_id} if doc_id is not None else {"identity.stfDecisionId": stf_decision_id},
        {"$set": update},
    )


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Iniciando etapa: OBTER HTML DO PROCESSO")

    try:
        col = get_case_data_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId nao informado.")
        return 1

    try:
        log("STEP", f"Buscando documento por identity.stfDecisionId='{stf_decision_id}'")
        doc = col.find_one({"identity.stfDecisionId": stf_decision_id})
    except PyMongoError as e:
        log("ERROR", f"Erro ao consultar documento: {e}")
        return 1

    doc_id = doc.get("_id") if doc else None
    case_url = None
    if doc:
        case_url = ((doc.get("caseContent") or {}).get("caseUrl") or "").strip()

    # Se nao existir documento, pedir URL ao usuario para permitir a coleta
    if not case_url:
        log("WARN", "caseContent.caseUrl nao encontrado no documento.")
        case_url = input("Informe a URL do processo (caseContent.caseUrl): ").strip()

    if not case_url:
        log("ERROR", "URL do processo nao informada.")
        if doc_id is not None:
            persist_error(col, stf_decision_id, doc_id=doc_id, err="caseUrl vazio")
        return 1

    try:
        log("STEP", f"Baixando HTML da URL: {case_url}")
        html = fetch_html(case_url)
        log("OK", f"HTML obtido | chars={len(html)}")

        log("STEP", "Persistindo HTML no MongoDB")
        persist_success(
            col,
            stf_decision_id,
            case_url=case_url,
            case_html=html,
            doc_id=doc_id,
        )
        log("OK", "Documento atualizado com sucesso")
        return 0

    except Exception as e:
        err = str(e)
        log("ERROR", f"Falha ao processar: {err}")
        try:
            persist_error(col, stf_decision_id, doc_id=doc_id, err=err)
        except Exception as inner:
            log("ERROR", f"Falha ao persistir erro: {inner}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
