#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step02-get-case-html.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Fetches full case HTML from STF and stores it in case_data.caseContent.caseHtml.
Inputs: config/mongo.yaml, identity.stfDecisionId, optional caseContent.caseUrl.
Outputs: case_data content + processing/status updates for case scrape (success/error/challenge).
Pipeline: load case URL -> HTTP fetch (requests/playwright fallback) -> persist HTML + metadata.
Dependencies: pymongo requests certifi
------------------------------------------------------------

"""

from __future__ import annotations

import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

import certifi
import requests
from pymongo.collection import Collection
from requests.adapters import HTTPAdapter
from requests.exceptions import SSLError
from urllib3.util.retry import Retry

from utils.mongo import get_case_data_collection

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
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"

COLLECTION_NAME = "case_data"


def get_collection() -> Collection:
    """Conecta no MongoDB e retorna a collection case_data."""
    return get_case_data_collection(MONGO_CONFIG_PATH, COLLECTION_NAME)


# =============================================================================
# 2) MONGO HELPERS
# =============================================================================

def find_case_by_decision_id(col: Collection, stf_decision_id: str) -> Optional[Dict[str, Any]]:
    """Busca um documento por identity.stfDecisionId com projection mínima."""
    return col.find_one(
        {"identity.stfDecisionId": stf_decision_id},
        projection={
            "_id": 1,
            "identity.stfDecisionId": 1,
            "identity.caseUrl": 1,
            "caseTitle": 1,
            "caseContent.caseUrl": 1,
        },
    )


def upsert_case_html_success(
    col: Collection,
    stf_decision_id: str,
    *,
    case_url: str,
    case_html: str,
    http_status: int,
    latency_ms: int,
) -> Tuple[bool, Any]:
    """
    Salva HTML em caseContent.caseHtml e atualiza status/audit/processing.
    Retorna (created, _id).
    """
    now = utc_now()
    update = {
        # Identidade mínima
        "identity.stfDecisionId": stf_decision_id,

        # Conteúdo
        "caseContent.caseUrl": case_url,
        "caseContent.caseHtml": case_html,

        # Processing (somente o necessário para esta etapa)
        "processing.caseScrapeStatus": "success",
        "processing.caseScrapeError": None,
        "processing.caseScrapeAt": now,
        "processing.caseScrapeHttpStatus": http_status,
        "processing.caseScrapeLatencyMs": latency_ms,
        "processing.caseScrapeHtmlBytes": len(case_html.encode("utf-8", errors="ignore")),
        "processing.pipelineStatus": "caseScraped",

        # Audit
        "audit.updatedAt": now,

        # Status
        "status.pipelineStatus": "caseScraped",
    }

    # createdAt apenas quando inserir
    set_on_insert = {
        "audit.createdAt": now,
    }

    res = col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": update, "$setOnInsert": set_on_insert},
        upsert=True,
    )

    created = bool(res.upserted_id)
    doc_id = res.upserted_id
    if not doc_id:
        # Se não inseriu, recupera _id do doc existente
        doc = col.find_one({"identity.stfDecisionId": stf_decision_id}, projection={"_id": 1})
        doc_id = doc.get("_id") if doc else None

    return created, doc_id


def update_case_html_error(
    col: Collection,
    stf_decision_id: str,
    *,
    error_msg: str,
    case_url: Optional[str] = None,
) -> None:
    """Em erro: atualizar apenas processing e audit (não mexe em status.pipelineStatus)."""
    now = utc_now()
    update: Dict[str, Any] = {
        "processing.caseScrapeStatus": "error",
        "processing.caseScrapeError": error_msg,
        "processing.caseScrapeAt": now,
        "processing.pipelineStatus": "caseScrapeError",
        "audit.updatedAt": now,
        # Não atualiza status.pipelineStatus em erro, conforme requisito
    }
    if case_url:
        update["processing.caseScrapeUrl"] = case_url

    col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {
            "$set": update,
            "$setOnInsert": {
                "identity.stfDecisionId": stf_decision_id,
                "audit.createdAt": now,
            },
        },
        upsert=True,
    )


def mark_case_requires_js(
    col: Collection,
    stf_decision_id: str,
    *,
    case_url: str,
    challenge_html: str,
    http_status: int,
    latency_ms: int,
) -> None:
    """
    Marca documento indicando que a página retornou um challenge que requer execução JS.
    Salvamos o HTML do challenge em processing.caseScrapeChallengeHtml (truncado) e não
    sobrescrevemos caseContent.caseHtml com o conteúdo inválido.
    """
    now = utc_now()
    truncated = (challenge_html or "")[:32 * 1024]  # 32KB cap
    update = {
        "identity.stfDecisionId": stf_decision_id,
        "processing.caseScrapeStatus": "challenge",
        "processing.caseScrapeChallenge": True,
        "processing.caseScrapeChallengeHtml": truncated,
        "processing.caseScrapeHttpStatus": http_status,
        "processing.caseScrapeLatencyMs": latency_ms,
        "processing.caseScrapeAt": now,
        "processing.pipelineStatus": "caseScrapeChallenge",
        "audit.updatedAt": now,
        # Não atualizamos status.pipelineStatus para "caseScraped" — a etapa não foi concluída.
    }
    col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": update, "$setOnInsert": {"audit.createdAt": now}},
        upsert=True,
    )


# =============================================================================
# 3) HTTP FETCH
# =============================================================================

def is_waf_challenge(html: str) -> bool:
    """Detecta padrões comuns de challenge do AWS WAF / bot-check."""
    if not html:
        return False
    low = html.lower()
    checks = (
        "awswafintegration",
        "awswaf",
        "challenge-container",
        "javascript is disabled",
        "window.gokuprops",
        "token.awswaf",
    )
    return any(c in low for c in checks)


def fetch_html_playwright(url: str, timeout_seconds: int = 60, headless: bool = True) -> Tuple[str, int, int]:
    """
    Fallback usando Playwright para executar JS e obter o HTML renderizado.
    Requer 'pip install playwright' e 'playwright install chromium'.
    """
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except Exception:
        log("WARN", "Playwright não está instalado. Instale com: pip install playwright && playwright install chromium")
        raise

    started = time.time()
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=headless, args=["--no-sandbox"])
            context = browser.new_context(
                user_agent="Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120 Safari/537.36",
                accept_downloads=False,
            )
            page = context.new_page()
            page.goto(url, wait_until="networkidle", timeout=timeout_seconds * 1000)
            # Pequena espera adicional para permitir que o challenge seja resolvido
            try:
                page.wait_for_load_state("networkidle", timeout=5000)
            except PlaywrightTimeoutError:
                pass
            content = page.content()
            browser.close()
    except Exception as e:
        raise RuntimeError(f"Playwright fetch failed: {e}") from e

    latency_ms = int((time.time() - started) * 1000)
    return content, 200, latency_ms


def fetch_html(url: str, timeout_seconds: int = 60) -> Tuple[str, int, int]:
    """
    Busca HTML completo via HTTP GET com retries e tratamento de SSLError.
    Retorna (html, http_status, latency_ms).
    """
    headers = {
        "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) CITO/1.0 (+https://example.invalid)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }

    session = requests.Session()
    retries = Retry(total=3, backoff_factor=0.5, status_forcelist=[429, 500, 502, 503, 504])
    session.mount("https://", HTTPAdapter(max_retries=retries))

    started = time.time()
    combined_cert = CONFIG_DIR / "combined_cacert.pem"
    if combined_cert.exists():
        verify = str(combined_cert)
        log("INFO", f"Using combined CA bundle: {verify}")
    else:
        verify = certifi.where()
        log("INFO", f"Using certifi bundle: {verify}")

    try:
        resp = session.get(url, headers=headers, timeout=timeout_seconds, verify=verify)
    except SSLError as e:
        verify_path = verify
        msg = (
            f"SSL error ao conectar {url}: {e}. Cert bundle usado: {verify_path}. "
            "Sugestões: execute 'pip install --upgrade certifi' e/ou instale 'ca-certificates' no sistema. "
            "Se o problema for chain incompleto, adicione o certificado intermediário em versions/poc-v-d33/config/combined_cacert.pem."
        )
        log("ERROR", msg)
        # Propaga o erro para o caller que marcará o documento como erro
        raise RuntimeError(msg) from e

    latency_ms = int((time.time() - started) * 1000)

    resp.raise_for_status()

    if not resp.encoding:
        resp.encoding = "utf-8"

    html = resp.text
    # Se detectarmos challenge WAF, tentar Playwright (se disponível)
    if is_waf_challenge(html):
        log("WARN", "WAF challenge detectado na resposta HTTP; tentando fallback com Playwright (executa JS).")
        try:
            return fetch_html_playwright(url, timeout_seconds=timeout_seconds, headless=True)
        except Exception as e:
            log("ERROR", f"Fallback Playwright falhou: {e} — retornando HTML original do requests.")
    return html, resp.status_code, latency_ms


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Pipeline: OBTER HTML DO PROCESSO")

    # 1) Mongo
    try:
        col = get_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    # 2) Input
    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId vazio.")
        return 1

    # 3) Obter caseUrl
    log("STEP", f"Buscando documento: identity.stfDecisionId='{stf_decision_id}'")
    doc = find_case_by_decision_id(col, stf_decision_id)

    case_url = None
    if doc:
        case_url = ((doc.get("caseContent") or {}).get("caseUrl") or "").strip()
        if not case_url:
            case_url = ((doc.get("identity") or {}).get("caseUrl") or "").strip()
        log("OK", f"Documento encontrado | _id={doc.get('_id')} | title='{doc.get('caseTitle') or ''}'")
    else:
        log("WARN", "Documento não encontrado. Será criado via upsert (se houver URL).")

    # Se não houver URL no documento, solicitar ao usuário (necessário para executar a etapa 3)
    if not case_url:
        log("WARN", "caseContent.caseUrl ausente no documento.")
        case_url = input("Informe a URL do processo (caseContent.caseUrl): ").strip()

    if not case_url:
        err = "URL do processo ausente (caseContent.caseUrl)."
        log("ERROR", err)
        update_case_html_error(col, stf_decision_id, error_msg=err, case_url=None)
        return 1

    # 4) Fetch HTML
    try:
        log("STEP", f"Requisitando URL do processo: {case_url}")
        html, http_status, latency_ms = fetch_html(case_url, timeout_seconds=60)
        if is_waf_challenge(html):
            log("WARN", "WAF challenge detectado na resposta HTTP; tentando fallback com Playwright (executa JS).")
            try:
                html, http_status, latency_ms = fetch_html_playwright(case_url, timeout_seconds=60, headless=True)
                log("OK", "Playwright retornou conteúdo renderizado.")
            except Exception as e:
                log("ERROR", f"Fallback Playwright falhou: {e} — marcando documento como 'requires_js'.")
                mark_case_requires_js(
                    col,
                    stf_decision_id,
                    case_url=case_url,
                    challenge_html=html,
                    http_status=http_status,
                    latency_ms=latency_ms,
                )
                return 1
        log("OK", f"HTML obtido | http={http_status} | latency={latency_ms}ms | chars={len(html)}")
    except Exception as e:
        err = f"Falha ao requisitar HTML: {e}"
        log("ERROR", err)
        update_case_html_error(col, stf_decision_id, error_msg=err, case_url=case_url)
        return 1

    # 5) Persistir no Mongo
    try:
        log("STEP", "Salvando HTML em caseContent.caseHtml (upsert)")
        created, doc_id = upsert_case_html_success(
            col,
            stf_decision_id,
            case_url=case_url,
            case_html=html,
            http_status=http_status,
            latency_ms=latency_ms,
        )
        if created:
            log("OK", f"Documento criado com HTML | _id={doc_id} | status.pipelineStatus='caseScraped'")
        else:
            log("OK", f"Documento atualizado com HTML | _id={doc_id} | status.pipelineStatus='caseScraped'")
        return 0
    except Exception as e:
        err = f"Falha ao persistir HTML no MongoDB: {e}"
        log("ERROR", err)
        update_case_html_error(col, stf_decision_id, error_msg=err, case_url=case_url)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
