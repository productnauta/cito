#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
d-get-case-html-scrape.py

PIPELINE: SCRAPING HTML COMPLETO DO PROCESSO (STF)

Requisitos atendidos:
1) Solicitar identity.stfDecisionId
2) Obter URL em caseContent.caseUrl
3) Acessar URL e coletar HTML completo (requests e/ou Playwright)
4) Persistir HTML em caseContent.caseHtml (upsert)
5) Atualizar processing/audit/status.pipelineStatus
   - Sucesso: status.pipelineStatus="caseScraped"
   - Erro: atualizar apenas processing e audit

Config:
- versions/poc-v-d33/config/mongo.json  (conexão Mongo)
- versions/poc-v-d33/config/scraping.json (parâmetros de scraping)

Dependências:
  pip install pymongo requests certifi playwright
  playwright install chromium
"""

from __future__ import annotations

import json
import os
import time
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import certifi
import requests
from pymongo import MongoClient
from pymongo.collection import Collection


# =============================================================================
# 0) LOG / TEMPO
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def ts_local() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{ts_local()}] [{level}] {msg}")


# =============================================================================
# 1) PATHS / CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
SCRAPING_CONFIG_PATH = CONFIG_DIR / "scraping.json"

COLLECTION_NAME = "case_data"


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


@dataclass(frozen=True)
class RequestsCfg:
    timeout_seconds: int
    verify_tls: bool
    allow_redirects: bool
    headers: Dict[str, str]


@dataclass(frozen=True)
class PlaywrightCfg:
    enabled: bool
    headless: bool
    timeout_ms: int
    wait_until: str
    wait_for_load_state: str
    extra_wait_ms: int
    context: Dict[str, Any]


@dataclass(frozen=True)
class ChallengeCfg:
    enabled: bool
    markers: Tuple[str, ...]


@dataclass(frozen=True)
class ScrapingCfg:
    prefer_method: str
    requests: RequestsCfg
    playwright: PlaywrightCfg
    challenge: ChallengeCfg


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    """
    Espera estrutura:
      { "mongo": { "uri": "...", "database": "..." } }
    """
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida em mongo.json.")
    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri:
        raise ValueError("Config inválida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config inválida: 'mongo.database' vazio.")
    return MongoCfg(uri=uri, database=db)


def build_scraping_cfg(raw: Dict[str, Any]) -> ScrapingCfg:
    s = raw.get("scraping")
    if not isinstance(s, dict):
        raise ValueError("Config inválida: chave 'scraping' ausente em scraping.json.")

    prefer_method = str(s.get("prefer_method") or "requests_then_playwright").strip()

    r = s.get("requests") or {}
    rcfg = RequestsCfg(
        timeout_seconds=int(r.get("timeout_seconds") or 60),
        verify_tls=bool(r.get("verify_tls") if r.get("verify_tls") is not None else True),
        allow_redirects=bool(r.get("allow_redirects") if r.get("allow_redirects") is not None else True),
        headers=dict(r.get("headers") or {}),
    )

    p = s.get("playwright") or {}
    pcfg = PlaywrightCfg(
        enabled=bool(p.get("enabled") if p.get("enabled") is not None else True),
        headless=bool(p.get("headless") if p.get("headless") is not None else True),
        timeout_ms=int(p.get("timeout_ms") or 60000),
        wait_until=str(p.get("wait_until") or "domcontentloaded"),
        wait_for_load_state=str(p.get("wait_for_load_state") or "networkidle"),
        extra_wait_ms=int(p.get("extra_wait_ms") or 0),
        context=dict(p.get("context") or {}),
    )

    c = s.get("challenge_detection") or {}
    ccfg = ChallengeCfg(
        enabled=bool(c.get("enabled") if c.get("enabled") is not None else True),
        markers=tuple(c.get("markers") or ()),
    )

    return ScrapingCfg(
        prefer_method=prefer_method,
        requests=rcfg,
        playwright=pcfg,
        challenge=ccfg,
    )


def get_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    cfg = build_mongo_cfg(load_json(MONGO_CONFIG_PATH))

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{COLLECTION_NAME}'")
    return client[cfg.database][COLLECTION_NAME]


# =============================================================================
# 2) HELPERS: DETECT CHALLENGE / FETCH
# =============================================================================

def is_challenge_page(html: str, cfg: ChallengeCfg) -> bool:
    """Detecta HTML de challenge (ex.: AWS WAF) por markers configuráveis."""
    if not cfg.enabled:
        return False
    if not html:
        return False
    h = html.lower()
    return any(m.lower() in h for m in cfg.markers)


def fetch_html_requests(url: str, cfg: RequestsCfg) -> Tuple[str, int, int]:
    """Fetch via requests (não executa JS)."""
    verify_opt: Any = certifi.where() if cfg.verify_tls else False

    # Suporte a CA customizada em ambientes com proxy corporativo:
    # Se REQUESTS_CA_BUNDLE ou SSL_CERT_FILE estiver setado, requests usa automaticamente,
    # mas aqui permitimos priorizar explicitamente (se desejar).
    ca_env = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
    if cfg.verify_tls and ca_env:
        verify_opt = ca_env

    started = time.time()
    resp = requests.get(
        url,
        headers=cfg.headers,
        timeout=cfg.timeout_seconds,
        verify=verify_opt,
        allow_redirects=cfg.allow_redirects,
    )
    latency_ms = int((time.time() - started) * 1000)

    resp.raise_for_status()

    if not resp.encoding:
        resp.encoding = "utf-8"

    return resp.text, resp.status_code, latency_ms


def fetch_html_playwright(url: str, cfg: PlaywrightCfg) -> Tuple[str, int, int]:
    """
    Fetch via Playwright (executa JS).
    Observação: requer libs do sistema instaladas no host/container.
    """
    if not cfg.enabled:
        raise RuntimeError("Playwright desabilitado em scraping.json")

    # Import local para não quebrar o script em ambientes sem playwright instalado
    from playwright.sync_api import sync_playwright  # pylint: disable=import-error

    started = time.time()
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=cfg.headless)
        context = browser.new_context(**cfg.context)
        page = context.new_page()

        resp = page.goto(url, wait_until=cfg.wait_until, timeout=cfg.timeout_ms)
        page.wait_for_load_state(cfg.wait_for_load_state, timeout=cfg.timeout_ms)

        if cfg.extra_wait_ms > 0:
            page.wait_for_timeout(cfg.extra_wait_ms)

        html = page.content()
        status = resp.status if resp is not None else 200

        context.close()
        browser.close()

    latency_ms = int((time.time() - started) * 1000)
    return html, status, latency_ms


def fetch_html(url: str, cfg: ScrapingCfg) -> Tuple[str, str, int, int]:
    """
    Orquestra o método de scraping conforme prefer_method:
    - "requests_only"
    - "playwright_only"
    - "requests_then_playwright" (default)
    - "playwright_then_requests"
    Retorna: (html, method_used, http_status, latency_ms)
    """
    prefer = (cfg.prefer_method or "requests_then_playwright").strip().lower()

    def _try_requests() -> Optional[Tuple[str, str, int, int]]:
        log("STEP", "Fetch HTML via requests")
        html, status, ms = fetch_html_requests(url, cfg.requests)
        if is_challenge_page(html, cfg.challenge):
            log("WARN", "Challenge detectado no HTML retornado por requests.")
            return None
        return html, "requests", status, ms

    def _try_playwright() -> Optional[Tuple[str, str, int, int]]:
        log("STEP", "Fetch HTML via Playwright")
        html, status, ms = fetch_html_playwright(url, cfg.playwright)
        if is_challenge_page(html, cfg.challenge):
            log("WARN", "Challenge detectado no HTML retornado por Playwright.")
            return None
        return html, "playwright", status, ms

    if prefer == "requests_only":
        r = _try_requests()
        if not r:
            raise RuntimeError("requests retornou challenge/HTML inválido (requests_only).")
        return r

    if prefer == "playwright_only":
        p = _try_playwright()
        if not p:
            raise RuntimeError("playwright retornou challenge/HTML inválido (playwright_only).")
        return p

    if prefer == "playwright_then_requests":
        p = _try_playwright()
        if p:
            return p
        r = _try_requests()
        if r:
            return r
        raise RuntimeError("Falha em Playwright e requests (ambos retornaram challenge/erro).")

    # default: requests_then_playwright
    r = _try_requests()
    if r:
        return r
    log("INFO", "Fallback: tentando Playwright após requests (challenge/HTML inválido).")
    p = _try_playwright()
    if p:
        return p
    raise RuntimeError("Falha em requests e Playwright (ambos retornaram challenge/HTML inválido).")


# =============================================================================
# 3) MONGO: GET URL / UPSERT HTML / STATUS
# =============================================================================

def find_case_url(col: Collection, stf_decision_id: str) -> Tuple[Optional[Any], str]:
    """
    Retorna (_id, case_url).
    - case_url pode ser vazio se não existir no doc.
    """
    doc = col.find_one(
        {"identity.stfDecisionId": stf_decision_id},
        projection={"_id": 1, "caseContent.caseUrl": 1, "caseTitle": 1},
    )
    if not doc:
        return None, ""
    case_url = ((doc.get("caseContent") or {}).get("caseUrl") or "").strip()
    return doc.get("_id"), case_url


def upsert_success(
    col: Collection,
    stf_decision_id: str,
    *,
    case_url: str,
    html: str,
    method_used: str,
    http_status: int,
    latency_ms: int,
) -> Tuple[bool, Any]:
    now = utc_now()

    update = {
        "identity.stfDecisionId": stf_decision_id,
        "caseContent.caseUrl": case_url,
        "caseContent.caseHtml": html,

        "processing.caseScrapeStatus": "success",
        "processing.caseScrapeError": None,
        "processing.caseScrapeAt": now,
        "processing.caseScrapeMethod": method_used,
        "processing.caseScrapeHttpStatus": http_status,
        "processing.caseScrapeLatencyMs": latency_ms,
        "processing.caseScrapeHtmlBytes": len(html.encode("utf-8", errors="ignore")),

        "audit.updatedAt": now,

        "status.pipelineStatus": "caseScraped",
    }

    set_on_insert = {"audit.createdAt": now}

    res = col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": update, "$setOnInsert": set_on_insert},
        upsert=True,
    )

    created = bool(res.upserted_id)
    doc_id = res.upserted_id
    if not doc_id:
        doc = col.find_one({"identity.stfDecisionId": stf_decision_id}, projection={"_id": 1})
        doc_id = doc.get("_id") if doc else None

    return created, doc_id


def upsert_error(
    col: Collection,
    stf_decision_id: str,
    *,
    err: str,
    case_url: Optional[str],
    method_used: Optional[str] = None,
) -> None:
    """
    Em erro: atualizar apenas processing e audit (não mexe em status.pipelineStatus).
    Se o documento não existir, cria com identity.stfDecisionId + audit.createdAt.
    """
    now = utc_now()

    update: Dict[str, Any] = {
        "processing.caseScrapeStatus": "error",
        "processing.caseScrapeError": err,
        "processing.caseScrapeAt": now,
        "audit.updatedAt": now,
    }

    if case_url:
        update["processing.caseScrapeUrl"] = case_url
    if method_used:
        update["processing.caseScrapeMethod"] = method_used

    col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": update, "$setOnInsert": {"audit.createdAt": now, "identity.stfDecisionId": stf_decision_id}},
        upsert=True,
    )


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Pipeline: SCRAPING HTML COMPLETO DO PROCESSO")

    # A) Mongo
    try:
        col = get_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        traceback.print_exc()
        return 1

    # B) Scraping config
    try:
        log("STEP", f"Lendo config scraping: {SCRAPING_CONFIG_PATH.resolve()}")
        scraping_cfg = build_scraping_cfg(load_json(SCRAPING_CONFIG_PATH))
    except Exception as e:
        log("ERROR", f"Falha ao carregar scraping.json: {e}")
        traceback.print_exc()
        return 1

    # 1) Input stfDecisionId
    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId vazio.")
        return 1

    # 2) Obter caseUrl do documento
    log("STEP", f"Buscando caseContent.caseUrl para identity.stfDecisionId='{stf_decision_id}'")
    doc_id, case_url = find_case_url(col, stf_decision_id)

    if doc_id:
        log("OK", f"Documento encontrado | _id={doc_id}")
    else:
        log("WARN", "Documento não encontrado. Upsert será usado (documento será criado no sucesso).")

    if not case_url:
        log("ERROR", "caseContent.caseUrl ausente no documento. Não é possível scrapear sem URL.")
        upsert_error(col, stf_decision_id, err="caseContent.caseUrl ausente no documento.", case_url=None)
        return 1

    # 3) Scraping do HTML completo
    try:
        log("STEP", f"Iniciando scraping | url={case_url}")
        html, method_used, http_status, latency_ms = fetch_html(case_url, scraping_cfg)
        log("OK", f"HTML obtido | method={method_used} | http={http_status} | latency={latency_ms}ms | chars={len(html)}")
    except Exception as e:
        err = f"Falha ao requisitar/scrapear HTML: {e}"
        log("ERROR", err)
        traceback.print_exc()
        upsert_error(col, stf_decision_id, err=err, case_url=case_url)
        return 1

    # 4) Persistir HTML em caseContent.caseHtml (create/update)
    try:
        log("STEP", "Persistindo HTML em caseContent.caseHtml (upsert)")
        created, saved_id = upsert_success(
            col,
            stf_decision_id,
            case_url=case_url,
            html=html,
            method_used=method_used,
            http_status=http_status,
            latency_ms=latency_ms,
        )
        if created:
            log("OK", f"Documento criado e atualizado | _id={saved_id} | status.pipelineStatus='caseScraped'")
        else:
            log("OK", f"Documento atualizado | _id={saved_id} | status.pipelineStatus='caseScraped'")
        return 0
    except Exception as e:
        err = f"Falha ao salvar HTML no MongoDB: {e}"
        log("ERROR", err)
        traceback.print_exc()
        upsert_error(col, stf_decision_id, err=err, case_url=case_url, method_used="persist")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
