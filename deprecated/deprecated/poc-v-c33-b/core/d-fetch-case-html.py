#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
d_fetch_case_html_only_persist.py

ETAPA: OBTER HTML DOS PROCESSOS + PERSISTIR NO case_data (SEM sanitizar/MD/extrair)

Fluxo:
1) Claim atômico do próximo documento elegível em case_data
   - identity.stfDecisionId obrigatório
   - caseContent.caseUrl obrigatório (fallback: stfCard.caseUrl)
   - status de entrada: configurável (default: "extracted")
   - por padrão, só processa se não existir caseContent.originalHtml (a menos que force_refetch=True)
2) Buscar HTML do processo (requests)
3) Persistir HTML bruto em caseContent.originalHtml
4) Atualizar auditoria e status (ok/error)

Config:
- ./config/mongo.json  (obrigatório)
- ./config/query.json  (opcional; headers/user-agent e opções)

Dependências:
  pip install pymongo requests certifi
"""

from __future__ import annotations

import json
import sys
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import certifi
import requests
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# =============================================================================
# 0) PATHS CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
QUERY_CONFIG_PATH = CONFIG_DIR / "query.json"


# =============================================================================
# 1) LOG / TIME
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


def step(n: int, total: int, msg: str) -> None:
    log("STEP", f"({n}/{total}) {msg}")


def size_kb(text: Optional[str]) -> int:
    if not text:
        return 0
    return int(ceil(len(text.encode("utf-8")) / 1024))


# =============================================================================
# 2) CONFIG LOADERS
# =============================================================================

def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def _get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Getter simples por path "a.b.c"."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str
    case_data_collection: str


@dataclass(frozen=True)
class PipelineCfg:
    # Claim / status
    input_statuses: Tuple[str, ...]
    processing_status: str
    ok_status: str
    error_status: str

    # Atualização do documento
    write_status_paths: Tuple[str, ...]  # ex: ("audit.pipelineStatus", "status.pipelineStatus")

    # Comportamento de reprocessamento
    force_refetch: bool

    # HTTP
    requests_timeout_sec: int
    ssl_verify: bool
    user_agent: str
    accept_language: str
    referer: str
    request_delay_sec: float


def build_mongo_cfg(mongo_raw: Dict[str, Any]) -> MongoCfg:
    m = mongo_raw.get("mongo", {}) or {}
    uri = str(m.get("uri") or "").strip()
    database = str(m.get("database") or "").strip()
    if not uri or not database:
        raise ValueError("mongo.json inválido: campos obrigatórios 'mongo.uri' e 'mongo.database'")

    collections = m.get("collections", {}) if isinstance(m.get("collections"), dict) else {}
    case_data_collection = str(collections.get("case_data") or "case_data")

    return MongoCfg(uri=uri, database=database, case_data_collection=case_data_collection)


def build_pipeline_cfg(query_raw: Optional[Dict[str, Any]]) -> PipelineCfg:
    """
    Lê parâmetros do query.json (quando existir), com defaults seguros.
    """
    qr = query_raw or {}

    input_statuses = tuple(_get(qr, "pipeline.case_html_fetch.input_statuses", ["extracted"]))
    processing_status = str(_get(qr, "pipeline.case_html_fetch.processing_status", "caseHtmlScraping"))
    ok_status = str(_get(qr, "pipeline.case_html_fetch.ok_status", "caseHtmlFetched"))
    error_status = str(_get(qr, "pipeline.case_html_fetch.error_status", "caseHtmlError"))

    write_status_paths = tuple(_get(
        qr,
        "pipeline.case_html_fetch.write_status_paths",
        ["audit.pipelineStatus", "status.pipelineStatus"],
    ))

    force_refetch = bool(_get(qr, "pipeline.case_html_fetch.force_refetch", False))

    requests_timeout_sec = int(_get(qr, "http.timeout_sec", 60))
    ssl_verify = bool(_get(qr, "http.ssl_verify", True))

    user_agent = str(_get(
        qr,
        "http.user_agent",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
    ))
    accept_language = str(_get(qr, "http.accept_language", "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"))
    referer = str(_get(qr, "http.referer", "https://jurisprudencia.stf.jus.br/"))

    request_delay_sec = float(_get(qr, "http.request_delay_sec", 0))

    return PipelineCfg(
        input_statuses=input_statuses,
        processing_status=processing_status,
        ok_status=ok_status,
        error_status=error_status,
        write_status_paths=write_status_paths,
        force_refetch=force_refetch,
        requests_timeout_sec=requests_timeout_sec,
        ssl_verify=ssl_verify,
        user_agent=user_agent,
        accept_language=accept_language,
        referer=referer,
        request_delay_sec=request_delay_sec,
    )


# =============================================================================
# 3) MONGO HELPERS
# =============================================================================

def get_case_data_collection(cfg: MongoCfg) -> Collection:
    log("INFO", "Conectando ao MongoDB...")
    client = MongoClient(cfg.uri)
    db = client[cfg.database]
    log("INFO", f"MongoDB conectado | db='{cfg.database}' | collection='{cfg.case_data_collection}'")
    return db[cfg.case_data_collection]


def _get_str(doc: Dict[str, Any], path: str) -> Optional[str]:
    v = _get(doc, path, None)
    if isinstance(v, str) and v.strip() and v.strip() != "N/A":
        return v.strip()
    return None


def claim_next_case(col: Collection, p: PipelineCfg) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do doc mais antigo apto para fetching do HTML.

    Critérios:
    - pipelineStatus ∈ input_statuses (audit.pipelineStatus OR status.pipelineStatus)
    - identity.stfDecisionId válido
    - caseContent.caseUrl OU stfCard.caseUrl válido
    - por padrão: só claim se não houver caseContent.originalHtml (a menos que force_refetch=True)
    """
    base_filter: Dict[str, Any] = {
        "identity.stfDecisionId": {"$exists": True, "$nin": [None, "", "N/A"]},
        "$or": [
            {"caseContent.caseUrl": {"$exists": True, "$nin": [None, "", "N/A"]}},
            {"stfCard.caseUrl": {"$exists": True, "$nin": [None, "", "N/A"]}},
        ],
        "$or": [
            {"audit.pipelineStatus": {"$in": list(p.input_statuses)}},
            {"status.pipelineStatus": {"$in": list(p.input_statuses)}},
        ],
    }

    if not p.force_refetch:
        base_filter["$and"] = [
            {
                "$or": [
                    {"caseContent": {"$exists": False}},
                    {"caseContent.originalHtml": {"$exists": False}},
                    {"caseContent.originalHtml": None},
                    {"caseContent.originalHtml": ""},
                ]
            }
        ]

    set_update: Dict[str, Any] = {
        "processing.caseHtmlScrapingAt": utc_now(),
        "processing.caseHtmlError": None,
    }
    for spath in p.write_status_paths:
        set_update[spath] = p.processing_status

    return col.find_one_and_update(
        base_filter,
        {"$set": set_update},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_success(col: Collection, doc_id, p: PipelineCfg, *, original_html: str, http_status: int) -> None:
    now = utc_now()
    set_update: Dict[str, Any] = {
        "caseContent.originalHtml": original_html,
        "processing.caseHtmlScrapedAt": now,
        "processing.caseHtmlHttpStatus": int(http_status),
        "processing.caseHtmlError": None,
        "audit.updatedAt": now,
        "audit.lastExtractedAt": now,
    }
    for spath in p.write_status_paths:
        set_update[spath] = p.ok_status

    col.update_one({"_id": doc_id}, {"$set": set_update})


def mark_error(col: Collection, doc_id, p: PipelineCfg, *, error_msg: str) -> None:
    now = utc_now()
    set_update: Dict[str, Any] = {
        "processing.caseHtmlError": error_msg,
        "processing.caseHtmlScrapedAt": now,
        "audit.updatedAt": now,
    }
    for spath in p.write_status_paths:
        set_update[spath] = p.error_status

    col.update_one({"_id": doc_id}, {"$set": set_update})


# =============================================================================
# 4) FETCH HTML (requests)
# =============================================================================

def fetch_html_requests(url: str, p: PipelineCfg) -> Tuple[str, int]:
    headers = {
        "User-Agent": p.user_agent,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": p.accept_language,
        "Referer": p.referer,
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }

    verify_opt = certifi.where() if p.ssl_verify else False
    if not p.ssl_verify:
        try:
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        except Exception:
            pass

    resp = requests.get(url, headers=headers, timeout=p.requests_timeout_sec, verify=verify_opt)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text, resp.status_code


# =============================================================================
# 5) MAIN (processa 1 case_data por execução)
# =============================================================================

def main() -> int:
    total_steps = 7
    started_at = time.time()

    step(1, total_steps, "Carregando mongo.json")
    mongo_raw = load_json(MONGO_CONFIG_PATH)
    mongo_cfg = build_mongo_cfg(mongo_raw)

    step(2, total_steps, "Carregando query.json (opcional)")
    query_raw: Optional[Dict[str, Any]] = None
    try:
        query_raw = load_json(QUERY_CONFIG_PATH)
        log("INFO", f"query.json OK | path='{QUERY_CONFIG_PATH.resolve()}'")
    except FileNotFoundError:
        log("WARN", f"query.json não encontrado | path='{QUERY_CONFIG_PATH.resolve()}' (usando defaults)")

    pipe_cfg = build_pipeline_cfg(query_raw)

    step(3, total_steps, "Obtendo collection case_data")
    col = get_case_data_collection(mongo_cfg)

    step(4, total_steps, "Claim atômico do próximo processo elegível")
    doc = claim_next_case(col, pipe_cfg)
    if not doc:
        log("INFO", "Nenhum documento elegível para processamento (verifique status/urls/originalHtml).")
        return 0

    doc_id = doc["_id"]
    stf_id = _get_str(doc, "identity.stfDecisionId") or "N/A"
    case_title = _get_str(doc, "caseTitle") or _get_str(doc, "stfCard.caseTitle") or "N/A"

    log("INFO", f"Documento claimed | _id={doc_id} | stfDecisionId={stf_id}")
    log("INFO", f"Título: {case_title}")

    try:
        step(5, total_steps, "Identificando URL do processo (caseContent.caseUrl -> stfCard.caseUrl)")
        case_url = _get_str(doc, "caseContent.caseUrl") or _get_str(doc, "stfCard.caseUrl")
        if not case_url:
            raise ValueError("URL do processo ausente (caseContent.caseUrl/stfCard.caseUrl).")
        log("INFO", f"URL: {case_url}")

        step(6, total_steps, "Obtendo HTML completo da página do processo (requests)")
        html, status_code = fetch_html_requests(case_url, pipe_cfg)
        log("INFO", f"HTTP OK | status={status_code} | html_size={size_kb(html)} KB")

        step(7, total_steps, "Persistindo HTML bruto no case_data e marcando status OK")
        mark_success(col, doc_id, pipe_cfg, original_html=html, http_status=status_code)

        elapsed = time.time() - started_at
        log("INFO", f"Concluído | _id={doc_id} | stfDecisionId={stf_id} | tempo_total={elapsed:.2f}s")

        if pipe_cfg.request_delay_sec and pipe_cfg.request_delay_sec > 0:
            log("INFO", f"Aguardando delay entre requisições: {pipe_cfg.request_delay_sec:.2f}s")
            time.sleep(pipe_cfg.request_delay_sec)

        return 0

    except Exception as e:
        err = str(e)
        log("ERROR", f"Falha no processamento | _id={doc_id} | stfDecisionId={stf_id} | erro={err}")
        log("ERROR", "Stacktrace completo:")
        print(traceback.format_exc())

        mark_error(col, doc_id, pipe_cfg, error_msg=err)
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PyMongoError as e:
        log("ERROR", f"Erro MongoDB: {e}")
        sys.exit(2)
