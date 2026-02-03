#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step00-search-stf.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Searches STF jurisprudence, captures HTML, and stores raw HTML in case_query.
Inputs: config/mongo.yaml, config/query.yaml
Outputs: case_query.htmlRaw + status updates
Pipeline: load config -> build URL -> Playwright fetch -> persist HTML
Dependencies: pymongo, playwright, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List
from urllib.parse import urlencode, urlunparse

import yaml
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright
from pymongo.collection import Collection

from utils.mongo import get_mongo_client

# =============================================================================
# 0) LOG
# =============================================================================

def _ts() -> str:
    # Requisito: yy-mm-dd hh:mm:ss
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
QUERY_CONFIG_PATH = CONFIG_DIR / "query.yaml"

CASE_QUERY_COLLECTION = "case_query"

USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
VIEWPORT_SIZE = {"width": 1280, "height": 800}
LOCALE: str = "pt-BR"


@dataclass(frozen=True)
class QueryCfg:
    query_id: int
    status: str
    search_term: str
    full_text: bool
    page: int
    page_size: int
    sort_field: str
    sort_order: str
    request_delay_seconds: float
    ssl_verify: bool
    headed_mode: bool
    output_dir: Path
    url_scheme: str
    url_host: str
    url_path: str
    base: str
    synonym: bool
    plural: bool
    stems: bool
    exact_search: bool
    process_class_sigla: List[str]


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str
    case_query_collection: str
    status_input: str


def load_yaml(path: Path) -> Dict[str, Any]:
    # Carrega um YAML simples com validacao minima
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _as_bool(v: Any, default: bool = False) -> bool:
    if v is None:
        return default
    if isinstance(v, bool):
        return v
    return str(v).strip().lower() == "true"


def _as_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default


def build_query_cfg(raw: Dict[str, Any]) -> QueryCfg:
    q = raw.get("query") if isinstance(raw.get("query"), dict) else {}
    http = raw.get("http") if isinstance(raw.get("http"), dict) else {}
    runtime = raw.get("runtime") if isinstance(raw.get("runtime"), dict) else {}
    url = raw.get("url") if isinstance(raw.get("url"), dict) else {}
    fixed = raw.get("fixed_query_params") if isinstance(raw.get("fixed_query_params"), dict) else {}

    paging = q.get("paging") if isinstance(q.get("paging"), dict) else {}
    sorting = q.get("sorting") if isinstance(q.get("sorting"), dict) else {}
    text_flags = fixed.get("text_search_flags") if isinstance(fixed.get("text_search_flags"), dict) else {}
    filters = fixed.get("filters") if isinstance(fixed.get("filters"), dict) else {}

    sort_field = str(sorting.get("field") or sorting.get("sort") or "_score")
    sort_order = str(sorting.get("order") or sorting.get("sort_by") or "desc")

    search_term = str(q.get("search_term") or q.get("query_string") or "").strip()

    return QueryCfg(
        query_id=_as_int(q.get("id"), 1),
        status=str(q.get("status") or "active"),
        search_term=search_term,
        full_text=_as_bool(q.get("full_text"), True),
        page=_as_int(paging.get("page"), 1),
        page_size=_as_int(paging.get("page_size"), 50),
        sort_field=sort_field,
        sort_order=sort_order,
        request_delay_seconds=float(http.get("request_delay_seconds") or 0),
        ssl_verify=_as_bool(http.get("ssl_verify"), True),
        headed_mode=_as_bool(runtime.get("headed_mode"), False),
        output_dir=Path(str(runtime.get("output_dir") or "poc/v-a33-240125/data/html")),
        url_scheme=str(url.get("scheme") or "https"),
        url_host=str(url.get("host") or url.get("netloc") or "jurisprudencia.stf.jus.br"),
        url_path=str(url.get("path") or "/pages/search"),
        base=str(fixed.get("base") or "acordaos"),
        synonym=_as_bool(text_flags.get("synonym"), True),
        plural=_as_bool(text_flags.get("plural"), True),
        stems=_as_bool(text_flags.get("stems"), False),
        exact_search=_as_bool(text_flags.get("exact_search"), True),
        process_class_sigla=list(filters.get("process_class_sigla") or []),
    )


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    m = raw.get("mongo") if isinstance(raw.get("mongo"), dict) else {}
    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri or not db:
        raise ValueError("mongo.yaml invalido: 'mongo.uri' e 'mongo.database' sao obrigatorios.")

    collections = m.get("collections") if isinstance(m.get("collections"), dict) else {}
    statuses = m.get("pipeline_status") if isinstance(m.get("pipeline_status"), dict) else {}
    return MongoCfg(
        uri=uri,
        database=db,
        case_query_collection=str(collections.get("case_query") or CASE_QUERY_COLLECTION),
        status_input=str(statuses.get("input") or "new"),
    )


# =============================================================================
# 2) URL BUILDER
# =============================================================================

def _bool_param(v: bool) -> str:
    return "true" if v else "false"


def build_target_url(cfg: QueryCfg) -> str:
    # Parametros dinamicos
    dynamic_params = {
        "pesquisa_inteiro_teor": _bool_param(cfg.full_text),
        "pageSize": cfg.page_size,
        "queryString": cfg.search_term,
    }

    # Parametros fixos
    fixed_params = {
        "base": cfg.base,
        "sinonimo": _bool_param(cfg.synonym),
        "plural": _bool_param(cfg.plural),
        "radicais": _bool_param(cfg.stems),
        "buscaExata": _bool_param(cfg.exact_search),
        "page": cfg.page,
        "sort": cfg.sort_field,
        "sortBy": cfg.sort_order,
    }

    query_list = [(k, str(v)) for k, v in {**fixed_params, **dynamic_params}.items()]
    for sigla in cfg.process_class_sigla:
        query_list.append(("processo_classe_processual_unificada_classe_sigla", str(sigla)))

    query = urlencode(query_list)
    return urlunparse((cfg.url_scheme, cfg.url_host, cfg.url_path, "", query, ""))


# =============================================================================
# 3) SCRAPER + PERSISTENCE
# =============================================================================

def get_case_query_collection(cfg: MongoCfg) -> Collection:
    log("Conectando ao MongoDB")
    client, db_name = get_mongo_client(MONGO_CONFIG_PATH)
    log(f"MongoDB OK | db='{db_name}' | collection='{cfg.case_query_collection}'")
    return client[db_name][cfg.case_query_collection]


def _write_html_to_disk(output_dir: Path, html: str) -> None:
    # Salva HTML em disco, se possivel
    output_dir.mkdir(parents=True, exist_ok=True)
    filename = f"case_query_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    path = output_dir / filename
    path.write_text(html, encoding="utf-8")
    log(f"HTML salvo em disco: {path}")


def insert_case_query(
    collection: Collection,
    *,
    cfg: QueryCfg,
    url: str,
    html: str,
    status_input: str,
) -> str:
    # Documento base em case_query com status 'new'
    doc = {
        "extractionTimestamp": utc_now(),
        "queryId": cfg.query_id,
        "queryStatus": cfg.status,
        "queryString": cfg.search_term,
        "pageSize": cfg.page_size,
        "inteiroTeor": cfg.full_text,
        "queryUrl": url,
        "htmlRaw": html,
        "status": status_input,
        "audit": {"createdAt": utc_now(), "updatedAt": utc_now()},
    }

    log("Inserindo documento em case_query")
    result = collection.insert_one(doc)
    return str(result.inserted_id)


def scrape_html(url: str, cfg: QueryCfg) -> str:
    log("Iniciando Playwright")
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not cfg.headed_mode,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport=VIEWPORT_SIZE,
            locale=LOCALE,
            ignore_https_errors=not cfg.ssl_verify,
        )

        page = context.new_page()
        log("Navegando para a URL de busca (wait_until=networkidle)")
        page.goto(url, wait_until="networkidle")

        if cfg.request_delay_seconds > 0:
            log(f"Aguardando delay adicional: {cfg.request_delay_seconds}s")
            time.sleep(cfg.request_delay_seconds)

        html = page.content()
        browser.close()
        if not html:
            raise RuntimeError("HTML vazio retornado pela pagina.")
        return html


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INICIANDO EXECUCAO DO SCRIPT")

    # Carregar configuracoes
    log("Carregando configuracoes YAML")
    parser = argparse.ArgumentParser(description="Executa scraping STF com query.yaml.")
    parser.add_argument("--query-config", help="Caminho para o arquivo query.yaml")
    args = parser.parse_args()

    query_path = Path(args.query_config) if args.query_config else QUERY_CONFIG_PATH
    try:
        query_cfg = build_query_cfg(load_yaml(query_path))
        mongo_cfg = build_mongo_cfg(load_yaml(MONGO_CONFIG_PATH))
    except Exception as e:
        log(f"Erro ao carregar configuracoes: {e}")
        return 1

    if not query_cfg.search_term:
        log("query.search_term vazio. Encerrando.")
        return 1

    # Montar URL
    log("Montando URL de busca")
    url = build_target_url(query_cfg)
    log(f"URL alvo: {url}")

    # Conectar MongoDB
    collection = get_case_query_collection(mongo_cfg)

    # Coletar HTML
    log("Coletando HTML via Playwright")
    try:
        html = scrape_html(url, query_cfg)
    except PlaywrightError as e:
        log(f"Erro no Playwright: {e}")
        return 1
    except Exception as e:
        log(f"Falha inesperada na coleta: {e}")
        return 1

    log(f"HTML coletado | chars={len(html)}")

    # Salvar em disco (opcional)
    try:
        _write_html_to_disk(query_cfg.output_dir, html)
    except Exception as e:
        log(f"Aviso: falha ao salvar HTML em disco: {e}")

    # Inserir no MongoDB
    inserted_id = insert_case_query(collection, cfg=query_cfg, url=url, html=html, status_input=mongo_cfg.status_input)
    log(f"Documento inserido | _id={inserted_id}")

    log("FINALIZADO COM SUCESSO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
