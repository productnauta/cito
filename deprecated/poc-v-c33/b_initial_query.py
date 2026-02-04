from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional
from urllib.parse import urlencode, urlunparse

from playwright.sync_api import sync_playwright, Error as PlaywrightError
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from a_load_configs import load_configs


# ==============================================================================
# 0) MONGO CONFIG 
# ==============================================================================

MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
MONGO_DB_NAME = "cito-v-a33-240125"
MONGO_COLLECTION_NAME = "raw_html"


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


def _step(n: int, total: int, msg: str) -> None:
    _log("STEP", f"({n}/{total}) {msg}")


def _size_kb(text: str) -> int:
    if text is None:
        return 0
    return int((len(text.encode("utf-8")) + 1023) / 1024)


def get_mongo_collection() -> Collection:
    _log("INFO", "Conectando ao MongoDB...")
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB_NAME]
    _log("INFO", f"MongoDB conectado | db='{MONGO_DB_NAME}' | collection='{MONGO_COLLECTION_NAME}'")
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
        "status": "new",
    }

    _log("INFO", "Inserindo documento no MongoDB (raw_html)...")
    result = collection.insert_one(doc)
    inserted_id = str(result.inserted_id)
    _log("INFO", f"Documento inserido com sucesso | _id={inserted_id}")
    return inserted_id


# ==============================================================================
# 1) DEFAULTS (VINDO DO GOOGLE SHEETS via load_configs())
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
    _log("INFO", "Carregando configurações via Google Sheets (a_load_configs.py)...")
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

    _log("INFO", "Configurações carregadas")
    _log("INFO", f"query_string='{default_query_string}' | page_size={default_page_size} | inteiro_teor={default_pesquisa_inteiro_teor} | headed_mode={default_headed_mode}")
    _log("INFO", f"url={default_url_scheme}://{default_url_netloc}{default_url_path}")

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
) -> str:
    """
    Executa a pesquisa no STF, coleta o HTML e grava no MongoDB (raw_html).
    Retorna o _id inserido.
    """
    total_steps = 5
    started_at = time.time()

    _log("INFO", "Iniciando fluxo de coleta (STF -> HTML -> MongoDB)")

    _step(1, total_steps, "Preparando conexão MongoDB")
    col = get_mongo_collection()

    _step(2, total_steps, "Exibindo parâmetros de busca")
    _log("INFO", f"URL alvo: {url}")
    _log("INFO", f"query_string='{query_string}' | page_size={page_size} | inteiro_teor={pesquisa_inteiro_teor} | headed_mode={headed_mode}")

    _step(3, total_steps, "Inicializando Playwright e navegando até a página de resultados")
    html: Optional[str] = None
    try:
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
            _log("INFO", "Navegando (wait_until=networkidle)...")
            page.goto(url, wait_until="networkidle")
            _log("INFO", "Aguardando estabilização adicional (sleep=3s)...")
            time.sleep(3)

            html = page.content()
            browser.close()

    except PlaywrightError as e:
        _log("ERROR", f"Falha no Playwright: {e}")
        raise
    except Exception as e:
        _log("ERROR", f"Falha inesperada durante navegação/coleta: {e}")
        raise

    if not html:
        raise RuntimeError("HTML vazio retornado pela coleta (page.content()).")

    _step(4, total_steps, "Validando tamanho do HTML coletado")
    kb = _size_kb(html)
    _log("INFO", f"HTML coletado | len={len(html)} chars | ~{kb} KB")

    _step(5, total_steps, "Persistindo HTML no MongoDB")
    try:
        inserted_id = insert_raw_html(
            collection=col,
            query_string=query_string,
            page_size=page_size,
            inteiro_teor_str=pesquisa_inteiro_teor,
            html_raw=html,
        )
    except PyMongoError as e:
        _log("ERROR", f"Falha ao inserir no MongoDB: {e}")
        raise

    elapsed = time.time() - started_at
    _log("INFO", f"Concluído | _id={inserted_id} | tempo_total={elapsed:.2f}s")
    return inserted_id


# ==============================================================================
# 4) MAIN
# ==============================================================================

def main() -> None:
    _log("INFO", "Montando URL de pesquisa do STF...")
    url_alvo = build_target_url(
        query_string=DEFAULT_QUERY_STRING,
        page_size=DEFAULT_PAGE_SIZE,
        pesquisa_inteiro_teor=DEFAULT_PESQUISA_INTEIRO_TEOR,
        url_scheme=DEFAULT_URL_SCHEME,
        url_netloc=DEFAULT_URL_NETLOC,
        url_path=DEFAULT_URL_PATH,
    )

    inserted_id = scrape_and_insert_html(
        url=url_alvo,
        headed_mode=DEFAULT_HEADED_MODE,
        query_string=DEFAULT_QUERY_STRING,
        page_size=DEFAULT_PAGE_SIZE,
        pesquisa_inteiro_teor=DEFAULT_PESQUISA_INTEIRO_TEOR,
    )

    _log("INFO", f"Execução finalizada com sucesso | _id={inserted_id}")


if __name__ == "__main__":
    main()
