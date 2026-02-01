#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
e_fetch_case_html.py

Implementações incluídas:
1) Critério de seleção controlado (não reprocessar HTML sem necessidade)
   - Default: processa apenas docs sem caseContent.originalHtml (ou vazio)
   - FORCE_REFETCH=true: permite reprocessar e sobrescrever originalHtml

2) Índices no MongoDB para o padrão de busca/claim
   - Cria (idempotente) índice composto: (status.pipelineStatus, _id)
   - Opcionalmente cria índice parcial para documentos sem originalHtml
     (habilite via env CREATE_PARTIAL_INDEX=true)

Fluxo:
- Claim atômico do doc mais antigo elegível:
    status.pipelineStatus: listExtracted|extracted -> caseScraping
- Fetch HTML via Playwright (principal) ou requests (opcional)
- Atualiza o doc:
    - caseContent.originalHtml (sempre sobrescreve quando selecionado)
    - processing.caseHtmlScrapedAt (UTC)
    - status.pipelineStatus: caseScraped
- Em erro:
    - processing.caseHtmlError
    - processing.caseHtmlScrapedAt (UTC)
    - status.pipelineStatus: caseScrapeError

Env vars:
- USE_REQUESTS_FIRST=true|false (default false)
- STF_SSL_VERIFY=true|false (default true)  # apenas requests
- FORCE_REFETCH=true|false (default false)
- CREATE_PARTIAL_INDEX=true|false (default false)
"""

import asyncio
import os
from datetime import datetime, timezone
from typing import Optional, Dict, Any, Tuple

import certifi
import requests
from markdownify import markdownify as md  # Install with: pip install markdownify
from math import ceil
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# ------------------------------------------------------------
# Mongo (fixo) [recomendado migrar para ENV]
# ------------------------------------------------------------
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"
COLLECTION = "case_data"

# Pipeline status (schema atual)
PIPELINE_INPUT = "listExtracted"   # ou "extracted" (fallback)
PIPELINE_PROCESSING = "caseScraping"
PIPELINE_OK = "caseScraped"
PIPELINE_ERROR = "caseScrapeError"

USER_AGENT = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on", "sim", "s")


USE_REQUESTS_FIRST = _env_bool("USE_REQUESTS_FIRST", False)
SSL_VERIFY = _env_bool("STF_SSL_VERIFY", True)
FORCE_REFETCH = _env_bool("FORCE_REFETCH", False)
CREATE_PARTIAL_INDEX = _env_bool("CREATE_PARTIAL_INDEX", False)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# ------------------------------------------------------------
# Mongo helpers
# ------------------------------------------------------------
def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


def ensure_indexes(col: Collection) -> None:
    """
    Cria índices (idempotente). Não quebra se já existirem.

    Índice principal para claim (sempre recomendado):
      - { "status.pipelineStatus": 1, "_id": 1 }

    Índice parcial opcional (mais seletivo) para docs sem originalHtml:
      - habilitar via CREATE_PARTIAL_INDEX=true
    """
    try:
        col.create_index(
            [("status.pipelineStatus", 1), ("_id", 1)],
            name="idx_claim_pipeline_id",
            background=True,
        )

        if CREATE_PARTIAL_INDEX:
            # Índice parcial para acelerar o modo default (sem HTML).
            # Cobre casos em que originalHtml não existe / null / vazio.
            col.create_index(
                [("status.pipelineStatus", 1), ("_id", 1)],
                name="idx_claim_pipeline_id_no_html_partial",
                background=True,
                partialFilterExpression={
                    "$or": [
                        {"caseContent": {"$exists": False}},
                        {"caseContent.originalHtml": {"$exists": False}},
                        {"caseContent.originalHtml": None},
                        {"caseContent.originalHtml": ""},
                    ]
                },
            )

    except Exception as e:
        # Não falhar o processamento por problemas de index
        print(f"⚠️ Aviso: falha ao garantir índices: {e}")


def _get_stf_decision_id(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("identity", {}).get("stfDecisionId")
    if isinstance(v, str) and v.strip() and v.strip() != "N/A":
        return v.strip()
    return None


def _get_case_url(doc: Dict[str, Any]) -> Optional[str]:
    v = doc.get("stfCard", {}).get("caseUrl")
    if isinstance(v, str) and v.strip() and v.strip() != "N/A":
        return v.strip()
    return None


def claim_oldest_extracted(col: Collection) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do documento mais antigo apto para scraping.

    Critérios base:
    - status.pipelineStatus em estado de entrada (PIPELINE_INPUT ou "extracted")
    - identity.stfDecisionId válido
    - stfCard.caseUrl válido

    Critério controlado:
    - FORCE_REFETCH=false (default): só claim se NÃO existir originalHtml (ou estiver vazio)
    - FORCE_REFETCH=true: ignora esse filtro e permite sobrescrever.
    """
    base_filter: Dict[str, Any] = {
        "status.pipelineStatus": {"$in": [PIPELINE_INPUT, "extracted"]},
        "identity.stfDecisionId": {"$exists": True, "$nin": [None, "", "N/A"]},
        "stfCard.caseUrl": {"$exists": True, "$nin": [None, "", "N/A"]},
    }

    if not FORCE_REFETCH:
        base_filter["$or"] = [
            {"caseContent": {"$exists": False}},
            {"caseContent.originalHtml": {"$exists": False}},
            {"caseContent.originalHtml": None},
            {"caseContent.originalHtml": ""},
        ]

    return col.find_one_and_update(
        base_filter,
        {
            "$set": {
                "status.pipelineStatus": PIPELINE_PROCESSING,
                "processing.caseHtmlScrapingAt": utc_now(),
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_success(col: Collection, doc_id, *, html: str) -> None:
    """
    Grava/atualiza:
    - caseContent.originalHtml (sempre sobrescreve quando selecionado)
    - processing.caseHtmlScrapedAt
    - status.pipelineStatus
    Limpa erro anterior, se existir.
    """
    col.update_one(
        {"_id": doc_id},
        {
            "$set": {
                "caseContent.originalHtml": html,
                "processing.caseHtmlScrapedAt": utc_now(),
                "status.pipelineStatus": PIPELINE_OK,
                "processing.caseHtmlError": None,
            }
        },
    )


def mark_error(col: Collection, doc_id, *, error_msg: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {
            "$set": {
                "processing.caseHtmlError": error_msg,
                "processing.caseHtmlScrapedAt": utc_now(),
                "status.pipelineStatus": PIPELINE_ERROR,
            }
        },
    )


def calculate_size_kb(content: str) -> int:
    """Calculate the size of the content in kilobytes."""
    return ceil(len(content.encode("utf-8")) / 1024)


def sanitize_html_keep_formatting(html: str) -> str:
    """Sanitize HTML, keep only main content + formatting + links."""
    try:
        from bs4 import BeautifulSoup  # Optional but preferred for clean sanitization
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "iframe", "object", "embed"]):
            tag.decompose()
        # Keep only the STF decision tab content
        content = soup.find("div", class_="mat-tab-body-wrapper")
        if content is not None:
            soup = BeautifulSoup(str(content), "html.parser")

        # Remove all tags except formatting and links
        allowed = {
            "b", "strong", "i", "em", "u",
            "p", "br",
            "ul", "ol", "li",
            "h1", "h2", "h3", "h4", "h5", "h6",
            "a",
            "blockquote",
        }
        for tag in list(soup.find_all(True)):
            if tag.name not in allowed:
                tag.unwrap()
            elif tag.name != "a":
                tag.attrs = {}
            else:
                # keep only href on links
                href = tag.get("href")
                tag.attrs = {}
                if href:
                    tag["href"] = href

        html = str(soup)
    except Exception:
        # Fallback: remove script/style blocks via simple heuristics
        import re

        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)

    return html.strip()


def sanitize_and_convert_to_markdown(html: str) -> str:
    """Convert sanitized HTML to Markdown."""
    return md(
        html,
        heading_style="ATX",
        bullet="*",
        strong_em_symbol="*",
        strip=["script", "style", "noscript", "iframe", "object", "embed"],
    ).strip()


def update_audit_status(col: Collection, doc_id, status: str) -> None:
    """Update the audit.sourceStatus field."""
    col.update_one(
        {"_id": doc_id},
        {"$set": {"audit.sourceStatus": status}},
    )


async def process_item(col: Collection, doc: Dict[str, Any], auto_confirm: bool) -> None:
    """Process a single item."""
    doc_id = doc["_id"]
    stf_id = _get_stf_decision_id(doc)
    case_title = doc.get("stfCard", {}).get("caseTitle", "N/A")
    print(f"Processo: {case_title}")

    try:
        # Fetch HTML
        case_url = _get_case_url(doc)
        if USE_REQUESTS_FIRST:
            html = fetch_html_requests(case_url)[0]
        else:
            html = await fetch_html_playwright(case_url)
        print("Obter HTML da decisão:          OK")
        html_size_kb = calculate_size_kb(html)
        print(f"Tamanho html:                   {html_size_kb} kb")

        # Save original HTML
        mark_success(col, doc_id, html=html)
        print("Gravar HTML original:           OK")

        # Sanitize HTML (keep only main content + formatting)
        sanitized_html = sanitize_html_keep_formatting(html)
        sanitized_size_kb = calculate_size_kb(sanitized_html)
        col.update_one(
            {"_id": doc_id},
            {"$set": {"caseContent.sanitizedHtml": sanitized_html}},
        )
        print(f"Tamanho html sanitizado:        {sanitized_size_kb} kb")
        print("Gravar HTML sanitizado:         OK")

        # Convert to Markdown
        markdown = sanitize_and_convert_to_markdown(sanitized_html)
        print("Converter para Markdown:        OK")
        markdown_size_kb = calculate_size_kb(markdown)
        print(f"Tamanho markdown:               {markdown_size_kb} kb")

        # Save Markdown
        col.update_one(
            {"_id": doc_id},
            {"$set": {"caseContent.contentMd": markdown}},
        )
        print("Gravar markdown:                OK")

        # Update audit status
        update_audit_status(col, doc_id, "Processed")
        print("PROCESSAMENTO ITEM FINALIZADO")

    except Exception as e:
        print(f"Erro ao processar item {doc_id}: {e}")
        mark_error(col, doc_id, error_msg=str(e))


def get_processing_options(col: Collection) -> Tuple[int, int, int]:
    """Get processing options and counts."""
    total_items = col.count_documents({})
    new_items = col.count_documents({"caseContent.contentMd": {"$exists": False}})
    existing_items = total_items - new_items
    return total_items, new_items, existing_items


def user_prompt(total: int, new: int, existing: int) -> Tuple[int, bool]:
    """Prompt user for processing options."""
    print("\n-------------------------------------")
    print("OBTER E SANITIZAR HTML DAS DECISÕES")
    print("-------------------------------------")
    print(f"Total de itens: {total}")
    print(f"Novos: {new}")
    print(f"Existentes: {existing}")
    print("\n-------------------------------------")
    print("ESCOLHA UMA OPÇÃO")
    print("-------------------------------------")
    print("1 - PROCESSAR TUDO")
    print("2 - PROCESSAR NOVOS")
    print("3 - ATUALIZAR EXISTENTES")
    print("-------------------------------------")

    option = int(input("Escolha uma opção: "))
    auto_confirm = input("Processar todos automaticamente sem confirmação? (s/n): ").strip().lower() == "s"
    return option, auto_confirm


# ------------------------------------------------------------
# requests (opcional)
# ------------------------------------------------------------
def fetch_html_requests(url: str) -> Tuple[str, int]:
    headers = {
        "User-Agent": USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://jurisprudencia.stf.jus.br/",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
    }
    verify_opt = certifi.where() if SSL_VERIFY else False
    if not SSL_VERIFY:
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    resp = requests.get(url, headers=headers, timeout=60, verify=verify_opt)
    resp.raise_for_status()
    resp.encoding = resp.encoding or "utf-8"
    return resp.text, resp.status_code


# ------------------------------------------------------------
# Playwright (principal)
# ------------------------------------------------------------
async def fetch_html_playwright(url: str) -> str:
    try:
        from playwright.async_api import async_playwright
    except Exception as e:
        raise RuntimeError(
            "Playwright não disponível. Instale com: pip install playwright && playwright install"
        ) from e

    from contextlib import suppress

    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"],
        )
        context = await browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            extra_http_headers={"accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        page = await context.new_page()

        try:
            await page.goto(url, wait_until="networkidle", timeout=60_000)
            await page.wait_for_timeout(3000)
            return await page.content()

        except (asyncio.CancelledError, KeyboardInterrupt):
            raise

        finally:
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            with suppress(Exception):
                await browser.close()


# ------------------------------------------------------------
# Main
# ------------------------------------------------------------
async def main() -> int:
    col: Optional[Collection] = None

    try:
        col = get_collection()
        ensure_indexes(col)

        # Get processing options
        total, new, existing = get_processing_options(col)
        option, auto_confirm = user_prompt(total, new, existing)

        # Filter documents based on user choice
        if option == 1:
            filter_query = {}
        elif option == 2:
            filter_query = {"caseContent.contentMd": {"$exists": False}}
        elif option == 3:
            filter_query = {"caseContent.contentMd": {"$exists": True}}
        else:
            print("Opção inválida.")
            return 1

        docs = col.find(filter_query).sort("_id", 1)
        total_to_process = col.count_documents(filter_query)
        print(f"\n-------------------------------------")
        print(f"PROCESSAMENTO INICIADO - ITENS {total_to_process}")
        print(f"-------------------------------------")

        for i, doc in enumerate(docs, start=1):
            print(f"\nItem {i}/{total_to_process}: {doc['_id']}")
            await process_item(col, doc, auto_confirm)
            if not auto_confirm:
                confirm = input("Processar próximo item? (s/n): ").strip().lower()
                if confirm != "s":
                    break

        print("\n-------------------------------------")
        print("PROCESSAMENTO FINALIZADO")
        print("-------------------------------------")
        return 0

    except Exception as e:
        print(f"Erro: {e}")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    print(f"Exit code: {exit_code}")
