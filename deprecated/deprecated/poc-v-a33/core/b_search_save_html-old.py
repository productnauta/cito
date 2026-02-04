from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict
from urllib.parse import urlencode, urlunparse

from playwright.sync_api import sync_playwright, Error as PlaywrightError
from pymongo import MongoClient
from pymongo.collection import Collection

from a_load_configs import load_configs


# ==============================================================================
# 0) MONGO CONFIG
# ==============================================================================

MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "cito-v-a33-240125"
MONGO_COLLECTION_NAME = "raw_html"


def get_mongo_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    return db[MONGO_COLLECTION_NAME]


def str_to_bool(v: str) -> bool:
    return str(v).strip().lower() == "true"


def insert_raw_html(
    *,
    collection: Collection,
    query_string: str,
    page_size: int,
    inteiro_teor_str: str,
    html_raw: str,
) -> str:
    doc = {
        "extractionTimestamp": datetime.now(timezone.utc),
        "queryString": query_string,
        "pageSize": str(page_size),
        "inteiroTeor": str_to_bool(inteiro_teor_str),
        "htmlRaw": html_raw,

        # >>> CAMPO ADICIONADO <<<
        "status": "new",
    }

    result = collection.insert_one(doc)
    return str(result.inserted_id)


# ==============================================================================
# 1) DEFAULTS (AGORA VINDO DO GOOGLE SHEETS via load_configs())
# ==============================================================================

_HARD_DEFAULT_QUERY_STRING: str = "homoafetiva"
_HARD_DEFAULT_PAGE_SIZE: int = 30
_HARD_DEFAULT_INTEIRO_TEOR_BOOL: bool = True
_HARD_DEFAULT_HEADED_MODE: bool = False
_HARD_DEFAULT_OUTPUT_DIR: Path = Path("poc/v-a33-240125/data/html")
_HARD_DEFAULT_URL_SCHEME: str = "https"
_HARD_DEFAULT_URL_NETLOC: str = "jurisprudencia.stf.jus.br"
_HARD_DEFAULT_URL_PATH: str = "/pages/search"


def _bool_to_str(value: bool) -> str:
    return "true" if value else "false"


def _safe_int(v: Any, default: int) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if s == "":
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default


def load_defaults_from_sheet() -> Dict[str, Any]:
    cfg = load_configs()

    default_query_string = cfg.query_string or _HARD_DEFAULT_QUERY_STRING
    default_page_size = _safe_int(cfg.page_size, _HARD_DEFAULT_PAGE_SIZE)

    default_pesquisa_inteiro_teor = _bool_to_str(
        cfg.inteiro_teor if cfg.inteiro_teor is not None else _HARD_DEFAULT_INTEIRO_TEOR_BOOL
    )
    default_headed_mode = cfg.headed_mode if cfg.headed_mode is not None else _HARD_DEFAULT_HEADED_MODE

    default_output_dir = Path(cfg.output_dir) if cfg.output_dir else _HARD_DEFAULT_OUTPUT_DIR

    default_url_scheme = cfg.url_scheme or _HARD_DEFAULT_URL_SCHEME
    default_url_netloc = cfg.url_netloc or _HARD_DEFAULT_URL_NETLOC
    default_url_path = cfg.url_path or _HARD_DEFAULT_URL_PATH

    return {
        "DEFAULT_QUERY_STRING": default_query_string,
        "DEFAULT_PAGE_SIZE": default_page_size,
        "DEFAULT_PESQUISA_INTEIRO_TEOR": default_pesquisa_inteiro_teor,
        "DEFAULT_HEADED_MODE": default_headed_mode,
        "DEFAULT_OUTPUT_DIR": default_output_dir,
        "DEFAULT_URL_SCHEME": default_url_scheme,
        "DEFAULT_URL_NETLOC": default_url_netloc,
        "DEFAULT_URL_PATH": default_url_path,
    }


_DEFAULTS = load_defaults_from_sheet()

DEFAULT_QUERY_STRING: str = _DEFAULTS["DEFAULT_QUERY_STRING"]
DEFAULT_PAGE_SIZE: int = _DEFAULTS["DEFAULT_PAGE_SIZE"]
DEFAULT_PESQUISA_INTEIRO_TEOR: str = _DEFAULTS["DEFAULT_PESQUISA_INTEIRO_TEOR"]
DEFAULT_HEADED_MODE: bool = _DEFAULTS["DEFAULT_HEADED_MODE"]
DEFAULT_OUTPUT_DIR: Path = _DEFAULTS["DEFAULT_OUTPUT_DIR"]
DEFAULT_URL_SCHEME: str = _DEFAULTS["DEFAULT_URL_SCHEME"]
DEFAULT_URL_NETLOC: str = _DEFAULTS["DEFAULT_URL_NETLOC"]
DEFAULT_URL_PATH: str = _DEFAULTS["DEFAULT_URL_PATH"]


FIXED_QUERY_PARAMS: Dict[str, Any] = {
    "base": "acordaos",
    "sinonimo": "true",
    "plural": "true",
    "radicais": "false",
    "buscaExata": "true",
    "processo_classe_processual_unificada_classe_sigla": ["ADC", "ADI", "ADO", "ADPF"],
    "page": 1,
    "sort": "_score",
    "sortBy": "desc",
}

USER_AGENT: str = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)
VIEWPORT_SIZE = {"width": 1280, "height": 800}
LOCALE: str = "pt-BR"


# ==============================================================================
# 2) BUILD URL
# ==============================================================================

def build_target_url(
    *,
    query_string: str,
    page_size: int,
    pesquisa_inteiro_teor: str,
    url_scheme: str,
    url_netloc: str,
    url_path: str,
) -> str:
    dynamic_params = {
        "pesquisa_inteiro_teor": pesquisa_inteiro_teor,
        "pageSize": page_size,
        "queryString": query_string,
    }

    all_params = FIXED_QUERY_PARAMS.copy()
    classes = all_params.pop("processo_classe_processual_unificada_classe_sigla", [])
    all_params.update(dynamic_params)

    query_list = [(k, str(v)) for k, v in all_params.items()]
    for class_name in classes:
        query_list.append(("processo_classe_processual_unificada_classe_sigla", class_name))

    query = urlencode(query_list)
    return urlunparse((url_scheme, url_netloc, url_path, "", query, ""))


# ==============================================================================
# 3) SCRAPER
# ==============================================================================

def scrape_and_insert_html(
    url: str,
    headed_mode: bool,
    *,
    query_string: str,
    page_size: int,
    pesquisa_inteiro_teor: str,
) -> None:
    print(f"🔹 Iniciando raspagem do STF:\n{url}")
    inicio = time.time()

    col = get_mongo_collection()

    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not headed_mode,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )

        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport=VIEWPORT_SIZE,
            locale=LOCALE,
        )

        page = context.new_page()
        page.goto(url, wait_until="networkidle")
        time.sleep(3)

        html = page.content()
        browser.close()

    inserted_id = insert_raw_html(
        collection=col,
        query_string=query_string,
        page_size=page_size,
        inteiro_teor_str=pesquisa_inteiro_teor,
        html_raw=html,
    )

    duracao = time.time() - inicio
    print(f"🗃️ HTML salvo no MongoDB (_id={inserted_id})")
    print(f"⏱️ Tempo total: {duracao:.2f}s")


# ==============================================================================
# 4) MAIN
# ==============================================================================

def main() -> None:
    url_alvo = build_target_url(
        query_string=DEFAULT_QUERY_STRING,
        page_size=DEFAULT_PAGE_SIZE,
        pesquisa_inteiro_teor=DEFAULT_PESQUISA_INTEIRO_TEOR,
        url_scheme=DEFAULT_URL_SCHEME,
        url_netloc=DEFAULT_URL_NETLOC,
        url_path=DEFAULT_URL_PATH,
    )

    scrape_and_insert_html(
        url=url_alvo,
        headed_mode=DEFAULT_HEADED_MODE,
        query_string=DEFAULT_QUERY_STRING,
        page_size=DEFAULT_PAGE_SIZE,
        pesquisa_inteiro_teor=DEFAULT_PESQUISA_INTEIRO_TEOR,
    )


if __name__ == "__main__":
    main()
