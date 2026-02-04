#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c_extract_data_case_data_v2.py

Objetivo:
- Ler o documento mais antigo da collection "raw_html" com status="new" (claim atômico)
- Extrair os cards de resultado do STF (div.result-container)
- Persistir cada decisão na collection "case_data" NO FORMATO ESTRUTURA SOLICITADOS
- Se um dado não estiver disponível na fonte, o campo correspondente NÃO é criado
- Atualizar o status do raw_html: new -> extracting -> extracted (ou error)

Dependências:
pip install pymongo beautifulsoup4
"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# ==============================================================================
# 0) MONGO CONFIG (fixo)
# ==============================================================================

MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"

RAW_HTML_COLLECTION = "raw_html"
CASE_DATA_COLLECTION = "case_data"

RAW_STATUS_INPUT = "new"
RAW_STATUS_PROCESSING = "extracting"
RAW_STATUS_OK = "extracted"
RAW_STATUS_ERROR = "error"


# ==============================================================================
# Utils
# ==============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


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
    """
    Só cria o campo se houver valor "útil":
    - str não vazia
    - int/float != None (para ocorrências: decidir no chamador)
    - dict/list não vazios
    """
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


# ==============================================================================
# Mongo helpers
# ==============================================================================

def get_collections() -> Tuple[Collection, Collection]:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[RAW_HTML_COLLECTION], db[CASE_DATA_COLLECTION]


def claim_next_raw_html(raw_col: Collection) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do próximo raw_html com status=new.
    """
    return raw_col.find_one_and_update(
        {"status": RAW_STATUS_INPUT},
        {"$set": {"status": RAW_STATUS_PROCESSING, "extractingAt": utc_now()}},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_raw_ok(raw_col: Collection, raw_id, *, extracted_count: int) -> None:
    raw_col.update_one(
        {"_id": raw_id, "status": RAW_STATUS_PROCESSING},
        {"$set": {
            "status": RAW_STATUS_OK,
            "processedDate": utc_now(),
            "extractedCount": int(extracted_count),
        }},
    )


def mark_raw_error(raw_col: Collection, raw_id, *, error_msg: str) -> None:
    raw_col.update_one(
        {"_id": raw_id, "status": RAW_STATUS_PROCESSING},
        {"$set": {
            "status": RAW_STATUS_ERROR,
            "processedDate": utc_now(),
            "error": _clean_ws(error_msg),
        }},
    )


# ==============================================================================
# Extração dos cards (result-container)
# ==============================================================================

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
    # Heurística simples: achar qualquer nó com texto contendo "Relator", "Órgão julgador" etc.
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            # tenta pegar próximo span
            nxt = el.find_next("span")
            if nxt:
                return _clean_str(nxt.get_text(" ", strip=True))
            # tenta texto após ":"
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


def _extract_case_class(container, fallback_title: Optional[str]) -> Optional[str]:
    for a in container.find_all("a"):
        if a.has_attr("href") and "classe=" in a["href"]:
            parsed = urlparse(a["href"])
            qs = parse_qs(parsed.query)
            if "classe" in qs and qs["classe"]:
                return _clean_str(qs["classe"][0])
    # fallback por título
    if fallback_title:
        parts = fallback_title.split()
        if parts and re.fullmatch(r"[A-Z]{2,}", parts[0]):
            return parts[0]
    return None


def _extract_case_number(container, fallback_title: Optional[str]) -> Optional[str]:
    for a in container.find_all("a"):
        if a.has_attr("href") and "numeroProcesso=" in a["href"]:
            parsed = urlparse(a["href"])
            qs = parse_qs(parsed.query)
            if "numeroProcesso" in qs and qs["numeroProcesso"]:
                return _clean_str(qs["numeroProcesso"][0])
    if fallback_title:
        nums = re.findall(r"\d[\d\.\-]*", fallback_title)
        if nums:
            return nums[-1]
    return None


def _extract_occurrences(container, keyword: str) -> Optional[int]:
    # procura "Inteiro teor (X)" / "Indexação (Y)" em qualquer texto
    texts = container.find_all(string=re.compile(keyword, re.IGNORECASE))
    for t in texts:
        s = str(t)
        m = re.search(r"\((\d+)\)", s)
        if m:
            try:
                n = int(m.group(1))
                return n
            except Exception:
                return None
    return None


def _extract_dom_id(node) -> Optional[str]:
    if node and getattr(node, "attrs", None) and "id" in node.attrs:
        return _clean_str(node.attrs.get("id"))
    return None


def _derive_from_title(case_title: str) -> Dict[str, str]:
    """
    Deriva:
    - caseCode (aqui: o próprio título, se não houver outro padrão)
    - caseClassDetail (sigla inicial)
    - caseNumberDetail (primeiro número)
    """
    out: Dict[str, str] = {}

    title = _clean_ws(case_title)
    if not title:
        return out

    # caseCode (derivado do título)
    out["caseCode"] = title

    # classe (sigla)
    m_class = re.match(r"^([A-Z]{2,})\b", title)
    if m_class:
        out["caseClassDetail"] = m_class.group(1)

    # número (primeiro grupo numérico)
    m_num = re.search(r"\b(\d[\d\.\-]*)\b", title)
    if m_num:
        out["caseNumberDetail"] = m_num.group(1)

    return out


def extract_cards(html_raw: str, source_raw_id: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_raw, "html.parser")
    containers = _find_result_containers(soup)

    out_docs: List[Dict[str, Any]] = []
    for idx, container in enumerate(containers, start=1):
        case_title = _extract_case_title(container)
        stf_id = _extract_stf_decision_id(container)
        case_url = _extract_case_url(container)

        # Regra mínima: sem stfDecisionId não persiste
        if not stf_id:
            continue

        # -------------------------
        # Monta documento case_data
        # -------------------------
        now = utc_now()

        doc: Dict[str, Any] = {}

        # Top-level
        _set_if(doc, "caseTitle", case_title)

        # identity/
        identity_pairs: List[Tuple[str, Any]] = []
        identity_pairs.append(("stfDecisionId", stf_id))
        identity_pairs.append(("rawHtmlId", source_raw_id))

        if case_title:
            derived = _derive_from_title(case_title)
            identity_pairs.append(("caseCode", derived.get("caseCode")))
            identity_pairs.append(("caseClassDetail", derived.get("caseClassDetail")))
            identity_pairs.append(("caseNumberDetail", derived.get("caseNumberDetail")))
            # caseDecisionType: "a preencher" -> não cria aqui

        identity = _subdoc_if_any(identity_pairs)
        _set_if(doc, "identity", identity)

        # dates/
        judgment_date = _extract_date_by_regex(container, "Julgamento")
        publication_date = _extract_date_by_regex(container, "Publicação")
        dates = _subdoc_if_any([
            ("judgmentDate", judgment_date),
            ("publicationDate", publication_date),
        ])
        _set_if(doc, "dates", dates)

        # query/ (vem do raw_html)
        # suporta modelo novo (search.*) e modelo antigo (queryString/pageSize/inteiroTeor)
        query_sub = _subdoc_if_any([
            ("queryString", None),  # preenchido abaixo
            ("pageSize", None),
            ("inteiroTeor", None),
        ])
        query_sub = query_sub or {}

        # queryString
        _set_if(query_sub, "queryString", None)  # placeholder (não cria)
        # (preencher a partir do raw_doc no caller, via injection - ver build_query_from_raw)

        # caseContent/
        case_content = _subdoc_if_any([
            ("caseUrl", case_url),
            # rawHtml/sanitizedHtml só existem em etapas posteriores -> não cria aqui
        ])
        _set_if(doc, "caseContent", case_content)

        # stfCard/
        judging_body = _extract_labeled_value(container, "Órgão julgador")
        rapporteur = _extract_labeled_value(container, "Relator")
        opinion_writer = _extract_labeled_value(container, "Redator")

        case_class = _extract_case_class(container, case_title)
        case_number = _extract_case_number(container, case_title)

        full_text_occ = _extract_occurrences(container, "Inteiro teor")
        indexing_occ = _extract_occurrences(container, "Indexação")

        dom_result_id = _extract_dom_id(container)

        stf_card: Dict[str, Any] = {}
        _set_if(stf_card, "localIndex", idx)
        _set_if(stf_card, "caseTitle", case_title)
        _set_if(stf_card, "caseUrl", case_url)
        _set_if(stf_card, "caseClass", case_class)
        _set_if(stf_card, "caseNumber", case_number)
        _set_if(stf_card, "judgingBody", judging_body)
        _set_if(stf_card, "rapporteur", rapporteur)
        _set_if(stf_card, "opinionWriter", opinion_writer)
        _set_if(stf_card, "judgmentDate", judgment_date)
        _set_if(stf_card, "publicationDate", publication_date)

        occ_sub: Dict[str, Any] = {}
        # Só cria se > 0 (0 aqui significa "não detectado" na prática)
        if isinstance(full_text_occ, int) and full_text_occ > 0:
            occ_sub["fullText"] = full_text_occ
        if isinstance(indexing_occ, int) and indexing_occ > 0:
            occ_sub["indexing"] = indexing_occ
        _set_if(stf_card, "occurrences", occ_sub or None)

        _set_if(stf_card, "domResultContainerId", dom_result_id)
        # domClipboardId (se não achar, não cria)
        dom_clip = None
        for b in container.find_all("button"):
            if b.has_attr("id") and b.has_attr("mattooltip"):
                tip = (b.get("mattooltip") or "").lower()
                if any(w in tip for w in ("copiar", "copy", "link")):
                    dom_clip = _clean_str(b["id"])
                    break
        _set_if(stf_card, "domClipboardId", dom_clip)

        _set_if(doc, "stfCard", stf_card or None)

        # audit/
        audit: Dict[str, Any] = {}
        _set_if(audit, "extractionDate", now)
        _set_if(audit, "lastExtractedAt", now)
        _set_if(audit, "builtAt", now)
        _set_if(audit, "updatedAt", now)
        _set_if(audit, "sourceStatus", "extracted")
        _set_if(audit, "pipelineStatus", "extracted")
        _set_if(doc, "audit", audit)

        out_docs.append(doc)

    return out_docs


def build_query_from_raw(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retorna subdoc query/ a partir do raw_html (suporta dois formatos).
    Só inclui chaves com valor disponível.
    """
    # formato novo (build_raw_html_doc)
    search = raw_doc.get("search") if isinstance(raw_doc.get("search"), dict) else {}
    # formato antigo (b_search_save_html.py legado)
    q_old = raw_doc

    query_string = _clean_str(search.get("queryString")) or _clean_str(q_old.get("queryString"))
    page_size = search.get("pageSize")
    inteiro_teor = search.get("inteiroTeor")

    # pageSize antigo costuma ser string
    if page_size is None:
        ps = q_old.get("pageSize")
        if ps is not None:
            try:
                page_size = int(float(str(ps).strip().replace(",", ".")))
            except Exception:
                page_size = None

    if inteiro_teor is None and "inteiroTeor" in q_old:
        it = q_old.get("inteiroTeor")
        if isinstance(it, bool):
            inteiro_teor = it
        else:
            s = str(it).strip().lower()
            if s in ("true", "1", "yes", "y", "on", "sim", "s"):
                inteiro_teor = True
            elif s in ("false", "0", "no", "n", "off", "nao", "não"):
                inteiro_teor = False
            else:
                inteiro_teor = None

    query_sub = _subdoc_if_any([
        ("queryString", query_string),
        ("pageSize", page_size),
        ("inteiroTeor", inteiro_teor),
    ])
    return query_sub or {}


def upsert_case_data(
    case_col: Collection,
    *,
    doc: Dict[str, Any],
    stf_decision_id: str,
) -> None:
    """
    UPSERT por identity.stfDecisionId.
    - Se existir: $set + updatedAt/lastExtractedAt
    - Se não existir: cria com builtAt/created equivalents em audit
    """
    now = utc_now()

    # garante audit.updatedAt/lastExtractedAt
    audit = doc.get("audit") if isinstance(doc.get("audit"), dict) else {}
    audit["updatedAt"] = now
    audit["lastExtractedAt"] = now
    doc["audit"] = audit

    # Campos somente na criação (se quiser diferenciar)
    set_on_insert: Dict[str, Any] = {}
    # se audit.builtAt não foi setado por algum motivo
    if "builtAt" not in audit:
        set_on_insert["audit.builtAt"] = now

    case_col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {
            "$set": doc,
            "$setOnInsert": set_on_insert,
        },
        upsert=True,
    )


# ==============================================================================
# Main (processa 1 raw_html por execução)
# ==============================================================================

def main() -> int:
    raw_col, case_col = get_collections()

    raw_doc = claim_next_raw_html(raw_col)
    if not raw_doc:
        print(f"✅ Nenhum documento com status='{RAW_STATUS_INPUT}' em '{RAW_HTML_COLLECTION}'.")
        return 0

    raw_id = raw_doc["_id"]
    raw_id_str = str(raw_id)

    try:
        html_raw = (raw_doc.get("payload", {}).get("htmlRaw") if isinstance(raw_doc.get("payload"), dict) else None)
        if not html_raw:
            html_raw = raw_doc.get("htmlRaw")  # formato antigo

        html_raw = (html_raw or "").strip()
        if not html_raw:
            raise ValueError("Documento raw_html não possui HTML (payload.htmlRaw/htmlRaw vazio).")

        query_sub = build_query_from_raw(raw_doc)

        extracted_docs = extract_cards(html_raw, raw_id_str)

        # injeta query/ em cada doc (somente se houver valores)
        if query_sub:
            for d in extracted_docs:
                _set_if(d, "query", query_sub)

        # persiste
        for d in extracted_docs:
            identity = d.get("identity") if isinstance(d.get("identity"), dict) else {}
            stf_id = _clean_str(identity.get("stfDecisionId"))
            if not stf_id:
                continue
            upsert_case_data(case_col, doc=d, stf_decision_id=stf_id)

        mark_raw_ok(raw_col, raw_id, extracted_count=len(extracted_docs))

        print("🗃️ Extração concluída")
        print(f"   raw_html._id: {raw_id_str}")
        print(f"   decisões persistidas (case_data): {len(extracted_docs)}")
        return 0

    except Exception as e:
        mark_raw_error(raw_col, raw_id, error_msg=str(e))
        print(f"❌ Erro ao processar raw_html._id={raw_id_str}: {e}")
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PyMongoError as e:
        print(f"❌ Erro MongoDB: {e}")
        sys.exit(2)
