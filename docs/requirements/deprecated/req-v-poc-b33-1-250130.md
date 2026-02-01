{
  "caseData": {
    "caseNotes": [
      {
        "noteType": "stf_acordao",
        "descriptors": ["LICENÇA-MATERNIDADE", "UNIÃO HOMOAFETIVA"],
        "rawLine": "ADI 4277 (TP), ADPF 132 (TP).",
        "items": [
          {
            "itemType": "stf_case",
            "caseClass": "ADI",
            "caseNumber": "4277",
            "suffix": null,
            "orgTag": "TP",
            "rawRef": "ADI 4277 (TP)"
          }
        ]
      }
    ]
  }
}


{
  "_id": "65f0c9f0e1b2c3d4e5f67890",
  "caseStfId": "sjur12345",
  "caseIdentification": {
     "caseTitle": "ADI 7518 / ES - ESPÍRITO SANTO",
     "caseClassDetail": "ADI",
     "caseCode": "7518",
     "judgingBody": "Tribunal Pleno",
     "rapporteur": "Min. Gilmar Mendes",
     "caseUrl": "https://jurisprudencia.stf.jus.br/..."
  },
  "dates": {
     "judgmentDate": "16/09/2024",
     "publicationDate": "02/10/2024"
  },
  "caseContent": {
     "caseHtml": "<html>...</html>",
     "caseHtmlClean": "<div class=\"mat-tab-body-wrapper\">...</div>",
     "caseMarkdown": "#### Publicação\n..."
  },
  "rawData": {
     "rawPublication": "PROCESSO ELETRÔNICO\nDJe-s/n DIVULG 01-10-2024 PUBLIC 02-10-2024",
     "rawParties": "REQTE.(S): PROCURADORA-GERAL DA REPÚBLICA\nINTDO.(A/S): GOVERNADOR DO ESTADO DO ESPÍRITO SANTO",
     "rawSummary": "Ação direta de inconstitucionalidade. 2. Licença-parental...",
     "rawDecision": "Decisão ...",
     "rawKeywords": "NECESSIDADE, EXTINÇÃO, TRIBUNAL DO JÚRI, ...",
     "rawLegislation": "LEI-008112/1990 (RJU) ...",
     "rawNotes": "Observação ...",
     "rawDoctrine": "BARROSO, Luís Roberto..."
  },
  "caseData": {
     "caseParties": [
        { "partieType": "REQTE.(S)", "partieName": "PROCURADORA-GERAL DA REPÚBLICA" },
        { "partieType": "INTDO.(A/S)", "partieName": "GOVERNADOR DO ESTADO DO ESPÍRITO SANTO" }
     ],
     "caseKeywords": [
        "licença parental",
        "servidor público",
        "constitucionalidade"
     ],
     "caseDoctrineReferences": [
        {
          "author": "BARROSO, Luís Roberto",
          "publicationTitle": "O controle de constitucionalidade no direito brasileiro: exposição sistemática da doutrina e análise crítica da jurisprudência",
          "edition": "4 ed",
          "publicationPlace": "São Paulo",
          "publisher": "Saraiva",
          "year": 2009,
          "page": "181",
          "rawCitation": "BARROSO, Luís Roberto. O controle de constitucionalidade... p. 181."
        }
     ],
     "caseLegislationReferences": [
        {
          "jurisdictionLevel": "federal",
          "normType": "CF",
          "normIdentifier": "CF-1988",
          "normYear": 1988,
          "normDescription": "Constituição Federal",
          "normReferences": [
             {
                "articleNumber": 5,
                "isCaput": true,
                "incisoNumber": 3,
                "paragraphNumber": null,
                "isParagraphSingle": false,
                "letterCode": null
             }
          ]
        }
     ]
  },
  "processing": {
     "pipelineStatus": "enriched",
     "caseHtmlScrapedAt": "2026-01-26T22:10:00Z",
     "caseContentMinedAt": "2026-01-26T22:35:00Z",
     "caseDoctrineRefsAt": "2026-01-26T22:40:00Z",
     "caseLegislationRefsAt": "2026-01-26T22:41:00Z",
     "lastUpdatedAt": "2026-01-26T22:41:00Z",
     "errors": []
  },
  "status": {
     "pipelineStatus": "caseScraped"
  },
  "sourceIds": {
     "rawHtmlId": "65f0c9f0e1b2c3d4e5f11111"
  }
}
















#




Analise todo o código do script  k_unified_case_pipeline.py e:

Identifique apenas o código referente à realização da pesquisa inicial no site do STF (montar url com os parametros, obter html da página) e gravação do HTML da pagina de resultados na collection case_query.


#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
k_unified_case_pipeline.py

Pipeline unificado para:
1) Buscar resultados STF e salvar HTML da busca em case_query
2) Extrair cards e inserir/atualizar case_data (schema unificado)
3) Buscar HTML completo do processo, sanitizar, converter para Markdown
4) Minerar seções do Markdown e preencher rawData/caseData

Dependências:
  pip install pymongo beautifulsoup4 playwright requests certifi markdownify
"""

from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlencode, urlunparse, urlparse, parse_qs

import certifi
import requests
from bs4 import BeautifulSoup
from markdownify import markdownify as md
from playwright.sync_api import sync_playwright
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection

try:
    from a_load_configs import load_configs
except Exception:
    load_configs = None  # type: ignore


# =========================
# Mongo config
# =========================
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"

CASE_QUERY_COLLECTION = "case_query"
CASE_DATA_COLLECTION = "case_data"


# =========================
# Defaults / Search params
# =========================
_HARD_DEFAULT_QUERY_STRING: str = "homoafetiva"
_HARD_DEFAULT_PAGE_SIZE: int = 100
_HARD_DEFAULT_INTEIRO_TEOR_BOOL: bool = True
_HARD_DEFAULT_HEADED_MODE: bool = False
_HARD_DEFAULT_URL_SCHEME: str = "https"
_HARD_DEFAULT_URL_NETLOC: str = "jurisprudencia.stf.jus.br"
_HARD_DEFAULT_URL_PATH: str = "/pages/search"

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
    "Mozilla/5.0 (X11; Linux x86_64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

USE_REQUESTS_FIRST = os.getenv("USE_REQUESTS_FIRST", "false").strip().lower() in ("1", "true", "yes", "y", "on", "sim", "s")
SSL_VERIFY = os.getenv("STF_SSL_VERIFY", "true").strip().lower() in ("1", "true", "yes", "y", "on", "sim", "s")


# =========================
# Utils
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s == "N/A":
        return None
    return s


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _set_if(doc: Dict[str, Any], key: str, value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str):
        v = _clean_str(value)
        if v is None:
            return
        doc[key] = v
        return
    if isinstance(value, dict):
        if value:
            doc[key] = value
        return
    if isinstance(value, list):
        if value:
            doc[key] = value
        return
    doc[key] = value


def _subdoc_if_any(pairs: List[Tuple[str, Any]]) -> Optional[Dict[str, Any]]:
    out: Dict[str, Any] = {}
    for k, v in pairs:
        _set_if(out, k, v)
    return out or None


def calculate_size_kb(content: str) -> int:
    return int((len(content.encode("utf-8")) + 1023) / 1024)


# =========================
# Mongo helpers
# =========================

def get_collections() -> Tuple[Collection, Collection]:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[CASE_QUERY_COLLECTION], db[CASE_DATA_COLLECTION]


# =========================
# Config loader
# =========================

def load_defaults() -> Dict[str, Any]:
    defaults = {
        "query_string": _HARD_DEFAULT_QUERY_STRING,
        "page_size": _HARD_DEFAULT_PAGE_SIZE,
        "inteiro_teor": _HARD_DEFAULT_INTEIRO_TEOR_BOOL,
        "headed_mode": _HARD_DEFAULT_HEADED_MODE,
        "url_scheme": _HARD_DEFAULT_URL_SCHEME,
        "url_netloc": _HARD_DEFAULT_URL_NETLOC,
        "url_path": _HARD_DEFAULT_URL_PATH,
    }

    if load_configs is None:
        return defaults

    try:
        cfg = load_configs()
        if cfg.query_string:
            defaults["query_string"] = cfg.query_string
        if cfg.page_size is not None:
            try:
                defaults["page_size"] = int(cfg.page_size)
            except Exception:
                pass
        if cfg.inteiro_teor is not None:
            defaults["inteiro_teor"] = bool(cfg.inteiro_teor)
        if cfg.headed_mode is not None:
            defaults["headed_mode"] = bool(cfg.headed_mode)
        if cfg.url_scheme:
            defaults["url_scheme"] = cfg.url_scheme
        if cfg.url_netloc:
            defaults["url_netloc"] = cfg.url_netloc
        if cfg.url_path:
            defaults["url_path"] = cfg.url_path
    except Exception as e:
        log(f"Aviso: falha ao carregar configs do Sheets: {e}")

    return defaults


# =========================
# Stage 1: search STF
# =========================

def build_target_url(
    *,
    query_string: str,
    page_size: int,
    pesquisa_inteiro_teor: bool,
    url_scheme: str,
    url_netloc: str,
    url_path: str,
) -> str:
    dynamic_params = {
        "pesquisa_inteiro_teor": str(pesquisa_inteiro_teor).lower(),
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


def fetch_search_html(url: str, headed_mode: bool) -> str:
    with sync_playwright() as pw:
        browser = pw.chromium.launch(
            headless=not headed_mode,
            args=["--no-sandbox", "--disable-setuid-sandbox", "--disable-dev-shm-usage"],
        )
        context = browser.new_context(
            user_agent=USER_AGENT,
            viewport={"width": 1280, "height": 800},
            locale="pt-BR",
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle")
        time.sleep(3)
        html = page.content()
        browser.close()
        return html


def insert_case_query(
    col: Collection,
    *,
    query_string: str,
    page_size: int,
    inteiro_teor: bool,
    url: str,
    html_raw: str,
) -> str:
    doc = {
        "extractionTimestamp": utc_now(),
        "queryString": query_string,
        "pageSize": int(page_size),
        "inteiroTeor": bool(inteiro_teor),
        "url": url,
        "htmlRaw": html_raw,
        "status": "new",
    }
    result = col.insert_one(doc)
    return str(result.inserted_id)


# =========================
# Card extraction (search results)
# =========================

def _find_result_containers(soup: BeautifulSoup):
    return soup.find_all("div", class_="result-container")


def _extract_stf_decision_id(container) -> Optional[str]:
    link = container.find("a", class_="mat-tooltip-trigger")
    if link and link.has_attr("href"):
        href = link["href"]
        parts = [p for p in href.split("/") if p]
        for part in reversed(parts):
            if part.startswith("sjur"):
                return part
        if parts:
            return parts[-1]
    return None


def _extract_case_title(container) -> Optional[str]:
    h4 = container.find("h4", class_="ng-star-inserted")
    if h4:
        return _clean_str(h4.get_text(" ", strip=True))
    link = container.find("a", class_="mat-tooltip-trigger")
    if link:
        h4_in = link.find("h4", class_="ng-star-inserted")
        if h4_in:
            return _clean_str(h4_in.get_text(" ", strip=True))
    return None


def _extract_case_url(container) -> Optional[str]:
    link = container.find("a", class_="mat-tooltip-trigger")
    if link and link.has_attr("href"):
        href = (link["href"] or "").strip()
        if not href:
            return None
        if href.startswith("http"):
            return href
        return f"https://jurisprudencia.stf.jus.br{href}"
    return None


def _extract_labeled_value(container, label_contains: str) -> Optional[str]:
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            nxt = el.find_next("span")
            if nxt:
                return _clean_str(nxt.get_text(" ", strip=True))
            if ":" in txt:
                return _clean_str(txt.split(":", 1)[1])
    return None


def _extract_date_by_regex(container, label_contains: str) -> Optional[str]:
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            m = re.search(r"\d{2}/\d{2}/\d{4}", txt)
            if m:
                return m.group(0)
            nxt = el.find_next("span")
            if nxt:
                return _clean_str(nxt.get_text(" ", strip=True))
    return None


def _derive_from_title(case_title: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    title = _clean_ws(case_title)
    if not title:
        return out
    m_class = re.match(r"^([A-Z]{2,})\b", title)
    if m_class:
        out["caseClassDetail"] = m_class.group(1)
    m_num = re.search(r"\b(\d[\d\.\-]*)\b", title)
    if m_num:
        out["caseCode"] = m_num.group(1)
    return out


def extract_cards(html_raw: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_raw, "html.parser")
    containers = _find_result_containers(soup)

    out_docs: List[Dict[str, Any]] = []
    for container in containers:
        case_title = _extract_case_title(container)
        stf_id = _extract_stf_decision_id(container)
        case_url = _extract_case_url(container)
        if not stf_id:
            continue

        judging_body = _extract_labeled_value(container, "Órgão julgador")
        rapporteur = _extract_labeled_value(container, "Relator")
        judgment_date = _extract_date_by_regex(container, "Julgamento")
        publication_date = _extract_date_by_regex(container, "Publicação")

        derived = _derive_from_title(case_title or "")

        out_docs.append({
            "caseStfId": stf_id,
            "caseTitle": case_title,
            "caseUrl": case_url,
            "caseClassDetail": derived.get("caseClassDetail"),
            "caseCode": derived.get("caseCode"),
            "judgingBody": judging_body,
            "rapporteur": rapporteur,
            "judgmentDate": judgment_date,
            "publicationDate": publication_date,
        })

    return out_docs


def upsert_case_minimal(case_col: Collection, card: Dict[str, Any], raw_id: str) -> None:
    case_stf_id = card.get("caseStfId")
    if not case_stf_id:
        return

    existing = case_col.find_one({"caseStfId": case_stf_id}, projection={"status.pipelineStatus": 1})

    case_ident = _subdoc_if_any([
        ("caseTitle", card.get("caseTitle")),
        ("caseClassDetail", card.get("caseClassDetail")),
        ("caseCode", card.get("caseCode")),
        ("judgingBody", card.get("judgingBody")),
        ("rapporteur", card.get("rapporteur")),
        ("caseUrl", card.get("caseUrl")),
    ])

    dates = _subdoc_if_any([
        ("judgmentDate", card.get("judgmentDate")),
        ("publicationDate", card.get("publicationDate")),
    ])

    doc: Dict[str, Any] = {
        "caseStfId": case_stf_id,
    }
    _set_if(doc, "caseIdentification", case_ident)
    _set_if(doc, "dates", dates)
    _set_if(doc, "sourceIds", {"rawHtmlId": raw_id})

    processing = {
        "lastUpdatedAt": utc_now(),
    }
    _set_if(doc, "processing", processing)

    if not existing or not (existing.get("status") or {}).get("pipelineStatus"):
        _set_if(doc, "status", {"pipelineStatus": "caseScraped"})
        _set_if(doc, "processing", {**processing, "pipelineStatus": "caseScraped"})

    case_col.update_one({"caseStfId": case_stf_id}, {"$set": doc}, upsert=True)


# =========================
# Stage 2: fetch case HTML
# =========================

def fetch_case_html_requests(url: str) -> str:
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
    return resp.text


def fetch_case_html_playwright(url: str) -> str:
    with sync_playwright() as p:
        browser = p.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-dev-shm-usage", "--disable-gpu", "--window-size=1920,1080"],
        )
        context = browser.new_context(
            viewport={"width": 1920, "height": 1080},
            user_agent=USER_AGENT,
            extra_http_headers={"accept-language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7"},
        )
        page = context.new_page()
        page.goto(url, wait_until="networkidle", timeout=60_000)
        time.sleep(2)
        html = page.content()
        browser.close()
        return html


def get_case_html(url: str) -> str:
    if USE_REQUESTS_FIRST:
        return fetch_case_html_requests(url)
    return fetch_case_html_playwright(url)


def sanitize_html_keep_formatting(html: str) -> str:
    try:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "iframe", "object", "embed"]):
            tag.decompose()
        content = soup.find("div", class_="mat-tab-body-wrapper")
        if content is not None:
            soup = BeautifulSoup(str(content), "html.parser")

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
                href = tag.get("href")
                tag.attrs = {}
                if href:
                    tag["href"] = href

        return str(soup).strip()
    except Exception:
        html = re.sub(r"<(script|style)[^>]*>.*?</\1>", "", html, flags=re.DOTALL | re.IGNORECASE)
        return html.strip()


def convert_to_markdown(html: str) -> str:
    return md(
        html,
        heading_style="ATX",
        bullet="*",
        strong_em_symbol="*",
        strip=["script", "style", "noscript", "iframe", "object", "embed"],
    ).strip()


# =========================
# Stage 3: parse markdown
# =========================

def parse_sections(md_text: str) -> Dict[str, str]:
    sections: Dict[str, List[str]] = {}
    current_title: Optional[str] = None

    for raw_line in (md_text or "").splitlines():
        line = raw_line.rstrip()
        m = re.match(r"^####\s+(.*)$", line)
        if m:
            current_title = _clean_ws(m.group(1))
            if current_title not in sections:
                sections[current_title] = []
            continue
        if current_title is not None:
            sections[current_title].append(line)

    out: Dict[str, str] = {}
    for title, lines in sections.items():
        content = "\n".join([ln for ln in lines]).strip()
        if content:
            out[title] = content
    return out


def parse_parties(text: str) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    for line in (text or "").splitlines():
        ln = _clean_ws(line)
        if not ln:
            continue
        if ":" in ln:
            left, right = ln.split(":", 1)
            p_type = _clean_ws(left)
            p_name = _clean_ws(right)
        else:
            p_type = ""
            p_name = ln
        if p_name:
            out.append({"partieType": p_type, "partieName": p_name})
    return out


def parse_keywords(text: str) -> List[str]:
    if not text:
        return []
    raw = text.replace("\n", ",")
    parts = [p.strip(" -") for p in raw.split(",")]
    return [p for p in parts if p]


def build_raw_and_case_data(sections: Dict[str, str]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    raw_data: Dict[str, Any] = {}
    case_data: Dict[str, Any] = {}

    for title, content in sections.items():
        if title == "Publicação":
            raw_data["rawPublication"] = content
        elif title == "Partes":
            raw_data["rawParties"] = content
            parties = parse_parties(content)
            if parties:
                case_data["caseParties"] = parties
        elif title in ("Ementa", "Resumo"):
            raw_data["rawSummary"] = content
        elif title == "Decisão":
            raw_data["rawDecision"] = content
        elif title in ("Indexação", "Palavras-chave", "Palavras-chave"):
            raw_data["rawKeywords"] = content
            keywords = parse_keywords(content)
            if keywords:
                case_data["caseKeywords"] = keywords
        elif title == "Legislação":
            raw_data["rawLegislation"] = content
        elif title == "Doutrina":
            raw_data["rawDoctrine"] = content
        elif title == "Observação":
            raw_data["rawNotes"] = content

    return raw_data, case_data


def extract_sections_from_markdown(md_text: str) -> Dict[str, Any]:
    sections: Dict[str, List[str]] = {}
    current_title: Optional[str] = None

    for raw_line in (md_text or "").splitlines():
        line = raw_line.rstrip()
        m = re.match(r"^####\s+(.*)$", line)
        if m:
            current_title = _clean_ws(m.group(1))
            if current_title not in sections:
                sections[current_title] = []
            continue
        if current_title is not None:
            sections[current_title].append(line)

    extracted_data: Dict[str, Any] = {}
    for title, lines in sections.items():
        content = "\n".join([ln for ln in lines]).strip()
        if not content:
            continue

        if title == "Publicação":
            extracted_data["caseContent.casePublication"] = content
        elif title == "Partes":
            extracted_data["caseContent.caseParties"] = content
        elif title == "Ementa":
            extracted_data["caseContent.caseSummary"] = content
        elif title == "Decisão":
            extracted_data["caseContent.caseDecision"] = content
        elif title == "Indexação":
            extracted_data["caseContent.caseKeywords"] = content
        elif title == "Legislação":
            extracted_data["caseContent.caseLegislation"] = content
        elif title == "Observação":
            extracted_data["caseContent.caseNotes"] = content
        elif title == "Doutrina":
            extracted_data["caseContent.caseDoctrine"] = content
        else:
            extracted_data[f"caseContent.{title}"] = content

    return extracted_data


# =========================
# Main
# =========================

def main() -> int:
    case_query_col, case_data_col = get_collections()

    defaults = load_defaults()
    url = build_target_url(
        query_string=defaults["query_string"],
        page_size=defaults["page_size"],
        pesquisa_inteiro_teor=defaults["inteiro_teor"],
        url_scheme=defaults["url_scheme"],
        url_netloc=defaults["url_netloc"],
        url_path=defaults["url_path"],
    )

    log("ETAPA 1: Pesquisar STF e identificar processos")
    log(f"URL: {url}")

    html_search = fetch_search_html(url, defaults["headed_mode"])
    query_id = insert_case_query(
        case_query_col,
        query_string=defaults["query_string"],
        page_size=defaults["page_size"],
        inteiro_teor=defaults["inteiro_teor"],
        url=url,
        html_raw=html_search,
    )
    log(f"HTML de busca salvo em case_query (_id={query_id})")

    cards = extract_cards(html_search)
    log(f"Processos identificados: {len(cards)}")

    for card in cards:
        upsert_case_minimal(case_data_col, card, query_id)

    # Contagem de novos vs existentes após etapa 1
    base_filter = {"status.pipelineStatus": "caseScraped"}
    new_filter = {
        **base_filter,
        "$or": [
            {"caseContent.caseHtml": {"$exists": False}},
            {"caseContent.caseHtml": ""},
        ],
    }
    existing_filter = {
        **base_filter,
        "caseContent.caseHtml": {"$exists": True, "$ne": ""},
    }

    total_case_scraped = case_data_col.count_documents(base_filter)
    new_count = case_data_col.count_documents(new_filter)
    existing_count = case_data_col.count_documents(existing_filter)

    print("\n-------------------------------------")
    print("PROCESSOS IDENTIFICADOS")
    print("-------------------------------------")
    print(f"Total caseScraped: {total_case_scraped}")
    print(f"Novos (sem HTML): {new_count}")
    print(f"Existentes (com HTML): {existing_count}")
    print("-------------------------------------")
    print("1 - Processar apenas novos")
    print("2 - Processar todos (novos + existentes)")
    print("-------------------------------------")

    opt = input("Escolha uma opção (1/2): ").strip()
    if opt not in {"1", "2"}:
        log("Opção inválida. Encerrando.")
        return 1

    print("\n-------------------------------------")
    print("CONFIRMAÇÃO POR ITEM")
    print("-------------------------------------")
    print("1 - Processar todos automaticamente")
    print("2 - Confirmar item a item")
    print("-------------------------------------")
    mode = input("Escolha uma opção (1/2): ").strip()
    if mode not in {"1", "2"}:
        log("Opção inválida. Encerrando.")
        return 1

    confirm_each = mode == "2"

    if opt == "1":
        filter_docs = new_filter
    else:
        filter_docs = base_filter

    docs = list(case_data_col.find(filter_docs).sort([("_id", 1)]))
    if not docs:
        log("Nenhum processo elegível para as próximas etapas.")
        return 0

    total = len(docs)
    print("\n-------------------------------------")
    print(f"INICIANDO PROCESSAMENTO DE {total} PROCESSOS")
    print("-------------------------------------")

    for i, doc in enumerate(docs, start=1):
        case_stf_id = doc.get("caseStfId")
        case_title = (doc.get("caseIdentification") or {}).get("caseTitle", "N/A")
        case_url = (doc.get("caseIdentification") or {}).get("caseUrl")

        if confirm_each:
            cont = input(f"Processar este processo? (s/n) _id={doc.get('_id')}: ").strip().lower()
            if cont != "s":
                continue

        print(f"\nItem {i}/{total}: {case_stf_id}")
        print(f"Processo: {case_title}")

        try:
            # Etapa 2: fetch HTML
            if not case_url:
                raise ValueError("caseUrl ausente")

            html = get_case_html(case_url)
            html_size = calculate_size_kb(html)
            print(f"Obter HTML da decisão:          OK")
            print(f"Tamanho html:                   {html_size} kb")

            sanitized = sanitize_html_keep_formatting(html)
            sanitized_size = calculate_size_kb(sanitized)
            print(f"Sanitizar HTML:                 OK")
            print(f"Tamanho html sanitizado:        {sanitized_size} kb")

            md_text = convert_to_markdown(sanitized)
            md_size = calculate_size_kb(md_text)
            print(f"Converter para Markdown:        OK")
            print(f"Tamanho markdown:               {md_size} kb")

            case_data_col.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "caseContent.caseHtml": html,
                    "caseContent.caseHtmlClean": sanitized,
                    "caseContent.caseMarkdown": md_text,
                    "status.pipelineStatus": "htmlFetched",
                    "processing.pipelineStatus": "htmlFetched",
                    "processing.caseHtmlScrapedAt": utc_now(),
                    "processing.lastUpdatedAt": utc_now(),
                }},
            )

            # Etapa 3: parse markdown
            sections = parse_sections(md_text)
            raw_data, case_data = build_raw_and_case_data(sections)

            update_fields: Dict[str, Any] = {
                "processing.caseContentMinedAt": utc_now(),
                "processing.lastUpdatedAt": utc_now(),
                "status.pipelineStatus": "enriched",
                "processing.pipelineStatus": "enriched",
            }
            if raw_data:
                update_fields["rawData"] = raw_data
            if case_data:
                update_fields["caseData"] = case_data

            case_data_col.update_one({"_id": doc["_id"]}, {"$set": update_fields})

            # Process Markdown sections
            process_case_markdown(case_data_col, doc)

            print("PROCESSAMENTO ITEM FINALIZADO")

        except Exception as e:
            err_msg = str(e)
            print(f"Erro no processamento: {err_msg}")
            case_data_col.update_one(
                {"_id": doc["_id"]},
                {"$set": {
                    "processing.lastUpdatedAt": utc_now(),
                    "processing.pipelineStatus": "error",
                    "status.pipelineStatus": "error",
                    "processing.errors": [err_msg],
                }},
            )

        if confirm_each:
            cont = input("Processar próximo item? (s/n): ").strip().lower()
            if cont != "s":
                break

    log("Processamento finalizado")
    return 0


def process_case_markdown(case_data_col: Collection, doc: Dict[str, Any]) -> None:
    case_id = doc.get("_id")
    markdown_content = (doc.get("caseContent") or {}).get("caseMarkdown")

    if not markdown_content:
        log(f"IGNORADO: {case_id} (caseMarkdown vazio)")
        return

    extracted_sections = extract_sections_from_markdown(markdown_content)

    update_fields = {"processing.lastUpdatedAt": utc_now()}
    update_fields.update(extracted_sections)

    case_data_col.update_one({"_id": case_id}, {"$set": update_fields})
    log(f"Processo {case_id} atualizado com seções extraídas do Markdown.")


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("Interrompido pelo usuário")
        sys.exit(130)
    except Exception as e:
        log(f"Erro fatal: {e}")
        sys.exit(1)











-
---------------------




# 
Crie um novo script, utilizando como base de referência o script m_doctrine_legislation_ai.py, para realizar o processamento do conteúdo do campo 

Desejo criar uma aplicação python web, para realizar a consulta de dados salvos na collection "case_data".

Filtros e visões de dados já mapeadas:

Identificar, contar e exibir lista de todos os processos com determinada citação nas caseDoctrineReferences.Permitindo informar como critérios um ou mais dos dados: 
    - nome do autor (correspondencia parcial em caseDoctrineReferences.author) 
    - título da publicação (correspondencia parcial em caseDoctrineReferences.publicationTitle

eXIBIR para o usuário na interface, uma tabela com os seguintes colunas:

autor - título da publicação - ocorrências (quantidade de processos contendo a citação) - detalhes (link que exibe a página de detalhamento)

A página de detalhamento, deve exibir uma lista de todos os processos contendo a correspondência.
Colunas:

caseTitle - caseStfId - caseUrl



{
  "_id": "65f0c9f0e1b2c3d4e5f67890",
  "caseStfId": "sjur12345",
  "caseIdentification": {
     "caseTitle": "ADI 7518 / ES - ESPÍRITO SANTO",
     "caseClassDetail": "ADI",
     "caseCode": "7518",
     "judgingBody": "Tribunal Pleno",
     "rapporteur": "Min. Gilmar Mendes",
     "caseUrl": "https://jurisprudencia.stf.jus.br/..."
  },
  "dates": {
     "judgmentDate": "16/09/2024",
     "publicationDate": "02/10/2024"
  },
  "caseContent": {
     "caseHtml": "<html>...</html>",
     "caseHtmlClean": "<div class=\"mat-tab-body-wrapper\">...</div>",
     "caseMarkdown": "#### Publicação\n..."
  },
  "rawData": {
     "rawPublication": "PROCESSO ELETRÔNICO\nDJe-s/n DIVULG 01-10-2024 PUBLIC 02-10-2024",
     "rawParties": "REQTE.(S): PROCURADORA-GERAL DA REPÚBLICA\nINTDO.(A/S): GOVERNADOR DO ESTADO DO ESPÍRITO SANTO",
     "rawSummary": "Ação direta de inconstitucionalidade. 2. Licença-parental...",
     "rawDecision": "Decisão ...",
     "rawKeywords": "NECESSIDADE, EXTINÇÃO, TRIBUNAL DO JÚRI, ...",
     "rawLegislation": "LEI-008112/1990 (RJU) ...",
     "rawNotes": "Observação ...",
     "rawDoctrine": "BARROSO, Luís Roberto..."
  },
  "caseData": {
     "caseParties": [
        { "partieType": "REQTE.(S)", "partieName": "PROCURADORA-GERAL DA REPÚBLICA" },
        { "partieType": "INTDO.(A/S)", "partieName": "GOVERNADOR DO ESTADO DO ESPÍRITO SANTO" }
     ],
     "caseKeywords": [
        "licença parental",
        "servidor público",
        "constitucionalidade"
     ],
     "caseDoctrineReferences": [
        {
          "author": "BARROSO, Luís Roberto",
          "publicationTitle": "O controle de constitucionalidade no direito brasileiro: exposição sistemática da doutrina e análise crítica da jurisprudência",
          "edition": "4 ed",
          "publicationPlace": "São Paulo",
          "publisher": "Saraiva",
          "year": 2009,
          "page": "181",
          "rawCitation": "BARROSO, Luís Roberto. O controle de constitucionalidade... p. 181."
        }
     ],
     "caseLegislationReferences": [
        {
          "jurisdictionLevel": "federal",
          "normType": "CF",
          "normIdentifier": "CF-1988",
          "normYear": 1988,
          "normDescription": "Constituição Federal",
          "normReferences": [
             {
                "articleNumber": 5,
                "isCaput": true,
                "incisoNumber": 3,
                "paragraphNumber": null,
                "isParagraphSingle": false,
                "letterCode": null
             }
          ]
        }
     ]
  },
  "processing": {
     "pipelineStatus": "enriched",
     "caseHtmlScrapedAt": "2026-01-26T22:10:00Z",
     "caseContentMinedAt": "2026-01-26T22:35:00Z",
     "caseDoctrineRefsAt": "2026-01-26T22:40:00Z",
     "caseLegislationRefsAt": "2026-01-26T22:41:00Z",
     "lastUpdatedAt": "2026-01-26T22:41:00Z",
     "errors": []
  },
  "status": {
     "pipelineStatus": "caseScraped"
  },
  "sourceIds": {
     "rawHtmlId": "65f0c9f0e1b2c3d4e5f11111"
  }
}