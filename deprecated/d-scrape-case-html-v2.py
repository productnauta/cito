#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
d-scrape-case-html-v2.py

Pipeline: Scraping do HTML completo da página do processo (STF)
- Lê config MongoDB em: versions/poc-v-d33/config/mongo.json
- Lê config de scraping em: versions/poc-v-d33/config/scraping.json  (criar conforme modelo anterior)
- Solicita identity.stfDecisionId
- Lê caseContent.caseUrl do documento
- Obtém HTML via requests e/ou Playwright (método Playwright replicando o padrão do e_fetch_case_html.py)
- Salva HTML em caseContent.caseHtml (upsert)
- Atualiza processing/audit/status.pipelineStatus
  - Sucesso: status.pipelineStatus="caseScraped"
  - Erro: atualiza apenas processing e audit
  - Caso "requires_js": atualiza processing e audit (sem alterar status.pipelineStatus)

Dependências:
  pip install pymongo requests certifi playwright
  playwright install chromium
"""

from __future__ import annotations

import asyncio
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
# 1) CONFIG PATHS
# =============================================================================

# Ajuste se você executar o script fora de versions/poc-v-d33
BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
SCRAPING_CONFIG_PATH = CONFIG_DIR / "scraping.json"

COLLECTION_NAME = "case_data"


# =============================================================================
# 2) CONFIG MODELS
# =============================================================================

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
    user_agent: str
    accept_language: str
    viewport_width: int
    viewport_height: int
    launch_args: Tuple[str, ...]


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
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida.")
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
        raise ValueError("Config inválida: chave 'scraping' ausente.")

    prefer_method = str(s.get("prefer_method") or "requests_then_playwright").strip().lower()

    r = s.get("requests") or {}
    rcfg = RequestsCfg(
        timeout_seconds=int(r.get("timeout_seconds") or 60),
        verify_tls=bool(r.get("verify_tls") if r.get("verify_tls") is not None else True),
        allow_redirects=bool(r.get("allow_redirects") if r.get("allow_redirects") is not None else True),
        headers=dict(r.get("headers") or {}),
    )

    p = s.get("playwright") or {}
    context = p.get("context") or {}
    extra_http_headers = context.get("extra_http_headers") or {}

    # Mantém os parâmetros anteriores, mas com o "método" (async + networkidle + wait) do exemplo.
    pcfg = PlaywrightCfg(
        enabled=bool(p.get("enabled") if p.get("enabled") is not None else True),
        headless=bool(p.get("headless") if p.get("headless") is not None else True),
        timeout_ms=int(p.get("timeout_ms") or 60_000),
        wait_until=str(p.get("wait_until") or "networkidle"),
        wait_for_load_state=str(p.get("wait_for_load_state") or "networkidle"),
        extra_wait_ms=int(p.get("extra_wait_ms") or 3000),
        user_agent=str(context.get("user_agent") or ""),
        accept_language=str(extra_http_headers.get("Accept-Language") or extra_http_headers.get("accept-language") or ""),
        viewport_width=int(context.get("viewport_width") or 1920),
        viewport_height=int(context.get("viewport_height") or 1080),
        launch_args=tuple(p.get("launch_args") or [
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--window-size=1920,1080",
        ]),
    )

    c = s.get("challenge_detection") or {}
    ccfg = ChallengeCfg(
        enabled=bool(c.get("enabled") if c.get("enabled") is not None else True),
        markers=tuple(c.get("markers") or ()),
    )

    # Fallback para UA/Language caso config não tenha preenchido
    if not pcfg.user_agent:
        # tenta derivar do requests header, se existir
        ua = rcfg.headers.get("User-Agent") or rcfg.headers.get("user-agent") or ""
        pcfg = PlaywrightCfg(**{**pcfg.__dict__, "user_agent": ua})

    if not pcfg.accept_language:
        al = rcfg.headers.get("Accept-Language") or rcfg.headers.get("accept-language") or "pt-BR,pt;q=0.9,en;q=0.8"
        pcfg = PlaywrightCfg(**{**pcfg.__dict__, "accept_language": al})

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
# 3) DETECÇÃO DE CHALLENGE + REGRAS DE FALLBACK
# =============================================================================

def is_challenge_page(html: str, challenge: ChallengeCfg) -> bool:
    if not challenge.enabled:
        return False
    if not html:
        return False
    h = html.lower()
    return any(m.lower() in h for m in challenge.markers)


def should_fallback_to_js(http_status: int, html: str, challenge: ChallengeCfg) -> bool:
    # 202/403/429 são frequentes em proteções/challenges/rate limit
    if http_status in (202, 403, 429):
        return True
    return is_challenge_page(html, challenge)


# =============================================================================
# 4) FETCH HTML (REQUESTS)
# =============================================================================

def fetch_html_requests(url: str, cfg: RequestsCfg) -> Tuple[str, int, int, str]:
    """
    Fetch via requests.
    - NÃO considera 202 como sucesso (retorna status para decisão do orquestrador).
    - TLS: tenta verify com certifi; se falhar e verify_tls=True, faz fallback verify=False (configurável por necessidade).
    """
    verify_opt: Any = certifi.where() if cfg.verify_tls else False
    ca_env = os.getenv("REQUESTS_CA_BUNDLE") or os.getenv("SSL_CERT_FILE")
    if cfg.verify_tls and ca_env:
        verify_opt = ca_env

    headers = cfg.headers or {}

    log("INFO", f"HTTP GET (requests) | verify={verify_opt!r}")
    started = time.time()

    try:
        resp = requests.get(
            url,
            headers=headers,
            timeout=cfg.timeout_seconds,
            verify=verify_opt,
            allow_redirects=cfg.allow_redirects,
        )
    except requests.exceptions.SSLError as e:
        if cfg.verify_tls:
            log("WARN", f"SSL erro, tentando fallback verify=False | err={e}")
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            resp = requests.get(
                url,
                headers=headers,
                timeout=cfg.timeout_seconds,
                verify=False,
                allow_redirects=cfg.allow_redirects,
            )
        else:
            raise

    latency_ms = int((time.time() - started) * 1000)

    # Só levanta para >=400. 202 será tratado acima.
    if resp.status_code >= 400:
        resp.raise_for_status()

    if not resp.encoding:
        resp.encoding = "utf-8"

    return resp.text, resp.status_code, latency_ms, "requests"


# =============================================================================
# 5) FETCH HTML (PLAYWRIGHT) — MÉTODO DO EXEMPLO (async + networkidle + wait)
# =============================================================================

async def fetch_html_playwright(url: str, cfg: PlaywrightCfg) -> Tuple[str, int, int, str]:
    """
    Método replicado do e_fetch_case_html.py:
    - playwright.async_api
    - chromium.launch(headless=..., args=[--no-sandbox, ...])
    - browser.new_context(viewport, user_agent, extra_http_headers)
    - page.goto(... wait_until="networkidle")
    - page.wait_for_timeout(extra_wait_ms)
    - page.content()
    """
    if not cfg.enabled:
        raise RuntimeError("Playwright desabilitado em scraping.json")

    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        raise RuntimeError("Playwright não disponível. Instale: pip install playwright && playwright install chromium") from e

    from contextlib import suppress

    log("INFO", f"HTTP GET (playwright) | headless={cfg.headless} | wait_until={cfg.wait_until}")
    started = time.time()

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=cfg.headless,
            args=list(cfg.launch_args),
        )

        context = await browser.new_context(
            viewport={"width": cfg.viewport_width, "height": cfg.viewport_height},
            user_agent=cfg.user_agent,
            extra_http_headers={"accept-language": cfg.accept_language},
        )

        page = await context.new_page()

        try:
            resp = await page.goto(url, wait_until=cfg.wait_until, timeout=cfg.timeout_ms)
            # Mantém compatibilidade com o exemplo: aguarda um pouco após networkidle
            if cfg.extra_wait_ms > 0:
                await page.wait_for_timeout(cfg.extra_wait_ms)

            html = await page.content()
            status = resp.status if resp is not None else 200

        finally:
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            with suppress(Exception):
                await browser.close()

    latency_ms = int((time.time() - started) * 1000)
    return html, status, latency_ms, "playwright"


# =============================================================================
# 6) ORQUESTRADOR: REQUESTS / PLAYWRIGHT
# =============================================================================

async def fetch_html(url: str, cfg: ScrapingCfg) -> Tuple[str, int, int, str]:
    prefer = cfg.prefer_method

    async def _try_requests() -> Optional[Tuple[str, int, int, str]]:
        html, status, ms, method = fetch_html_requests(url, cfg.requests)
        if should_fallback_to_js(status, html, cfg.challenge):
            log("WARN", f"requests retornou status={status} ou challenge (fallback JS necessário)")
            return None
        return html, status, ms, method

    async def _try_playwright() -> Optional[Tuple[str, int, int, str]]:
        html, status, ms, method = await fetch_html_playwright(url, cfg.playwright)
        if should_fallback_to_js(status, html, cfg.challenge):
            log("WARN", f"playwright retornou status={status} ou challenge (ainda não foi possível obter HTML real)")
            return None
        return html, status, ms, method

    if prefer == "requests_only":
        r = await _try_requests()
        if not r:
            raise RuntimeError("requests_only: HTML inválido/challenge/202-403-429.")
        return r

    if prefer == "playwright_only":
        p = await _try_playwright()
        if not p:
            raise RuntimeError("playwright_only: HTML inválido/challenge/202-403-429.")
        return p

    if prefer == "playwright_then_requests":
        p = await _try_playwright()
        if p:
            return p
        r = await _try_requests()
        if r:
            return r
        raise RuntimeError("Falha: Playwright e requests não retornaram HTML válido.")

    # default: requests_then_playwright
    r = await _try_requests()
    if r:
        return r

    log("INFO", "Fallback: tentando Playwright após requests")
    p = await _try_playwright()
    if p:
        return p

    raise RuntimeError("Falha: requests e Playwright não retornaram HTML válido.")


# =============================================================================
# 7) MONGO: GET URL / UPSERT RESULTADOS
# =============================================================================

def find_case_url(col: Collection, stf_decision_id: str) -> Tuple[Optional[Any], str]:
    doc = col.find_one(
        {"identity.stfDecisionId": stf_decision_id},
        projection={"_id": 1, "caseContent.caseUrl": 1, "caseTitle": 1},
    )
    if not doc:
        return None, ""
    return doc.get("_id"), (((doc.get("caseContent") or {}).get("caseUrl") or "").strip())


def upsert_success(
    col: Collection,
    stf_decision_id: str,
    *,
    case_url: str,
    html: str,
    http_status: int,
    latency_ms: int,
    method: str,
) -> Tuple[bool, Any]:
    now = utc_now()

    update = {
        "identity.stfDecisionId": stf_decision_id,
        "caseContent.caseUrl": case_url,
        "caseContent.caseHtml": html,

        "processing.caseScrapeStatus": "success",
        "processing.caseScrapeError": None,
        "processing.caseScrapeAt": now,
        "processing.caseScrapeHttpStatus": http_status,
        "processing.caseScrapeLatencyMs": latency_ms,
        "processing.caseScrapeMethod": method,
        "processing.caseScrapeHtmlBytes": len(html.encode("utf-8", errors="ignore")),

        "audit.updatedAt": now,
        "status.pipelineStatus": "caseScraped",
    }

    res = col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": update, "$setOnInsert": {"audit.createdAt": now}},
        upsert=True,
    )

    created = bool(res.upserted_id)
    doc_id = res.upserted_id
    if not doc_id:
        d = col.find_one({"identity.stfDecisionId": stf_decision_id}, projection={"_id": 1})
        doc_id = d.get("_id") if d else None

    return created, doc_id


def upsert_requires_js(col: Collection, stf_decision_id: str, *, case_url: str, reason: str) -> None:
    now = utc_now()
    col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": {
            "processing.caseScrapeStatus": "requires_js",
            "processing.caseScrapeError": reason,
            "processing.caseScrapeAt": now,
            "processing.caseScrapeUrl": case_url,
            "audit.updatedAt": now,
        }, "$setOnInsert": {
            "identity.stfDecisionId": stf_decision_id,
            "audit.createdAt": now,
        }},
        upsert=True,
    )


def upsert_error(col: Collection, stf_decision_id: str, *, case_url: Optional[str], err: str) -> None:
    now = utc_now()
    col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": {
            "processing.caseScrapeStatus": "error",
            "processing.caseScrapeError": err,
            "processing.caseScrapeAt": now,
            "processing.caseScrapeUrl": case_url,
            "audit.updatedAt": now,
        }, "$setOnInsert": {
            "identity.stfDecisionId": stf_decision_id,
            "audit.createdAt": now,
        }},
        upsert=True,
    )


# =============================================================================
# 8) MAIN (1 documento)
# =============================================================================

async def main() -> int:
    log("INFO", "SCRAPE CASE HTML (1 documento)")

    # Mongo
    try:
        col = get_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        traceback.print_exc()
        return 1

    # Scraping config
    try:
        log("STEP", f"Lendo config scraping: {SCRAPING_CONFIG_PATH.resolve()}")
        scraping_cfg = build_scraping_cfg(load_json(SCRAPING_CONFIG_PATH))
        log("OK", f"Config scraping carregada | prefer_method={scraping_cfg.prefer_method}")
    except Exception as e:
        log("ERROR", f"Falha ao carregar scraping.json: {e}")
        traceback.print_exc()
        return 1

    # Input
    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId vazio.")
        return 1

    # URL
    log("STEP", f"Buscando caseContent.caseUrl para identity.stfDecisionId='{stf_decision_id}'")
    doc_id, case_url = find_case_url(col, stf_decision_id)
    if not case_url:
        log("ERROR", "caseContent.caseUrl ausente no documento. Não é possível scrapear.")
        upsert_error(col, stf_decision_id, case_url=None, err="caseContent.caseUrl ausente no documento.")
        return 1

    log("OK", f"URL encontrada | _id={doc_id} | url={case_url}")

    # Fetch
    try:
        html, status, ms, method = await fetch_html(case_url, scraping_cfg)
        log("OK", f"HTML obtido | method={method} | http={status} | latency={ms}ms | chars={len(html)}")
    except Exception as e:
        # Se falhou por Playwright indisponível ou challenge, marcar requires_js quando aplicável
        err = str(e)

        # Heurística: se prefer_method envolve playwright e/ou requests retorna 202/challenge,
        # tratar como requires_js (principalmente se o erro mencionar libs do Playwright).
        requires_js_markers = [
            "requires_js",
            "playwright não disponível",
            "error while loading shared libraries",
            "challenge",
            "HTTP 202",
            "AwsWafIntegration",
            "token.awswaf.com",
        ]
        if any(m.lower() in err.lower() for m in requires_js_markers):
            log("WARN", f"Não foi possível obter HTML real (requires_js): {err}")
            upsert_requires_js(col, stf_decision_id, case_url=case_url, reason=err)
            return 1

        log("ERROR", f"Falha ao obter HTML: {err}")
        traceback.print_exc()
        upsert_error(col, stf_decision_id, case_url=case_url, err=err)
        return 1

    # Persistir
    try:
        log("STEP", "Persistindo HTML em caseContent.caseHtml (upsert)")
        created, saved_id = upsert_success(
            col,
            stf_decision_id,
            case_url=case_url,
            html=html,
            http_status=status,
            latency_ms=ms,
            method=method,
        )
        if created:
            log("OK", f"Documento criado | _id={saved_id} | status.pipelineStatus='caseScraped'")
        else:
            log("OK", f"Documento atualizado | _id={saved_id} | status.pipelineStatus='caseScraped'")
        return 0
    except Exception as e:
        err = f"Falha ao salvar HTML no MongoDB: {e}"
        log("ERROR", err)
        traceback.print_exc()
        upsert_error(col, stf_decision_id, case_url=case_url, err=err)
        return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
