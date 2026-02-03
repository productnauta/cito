#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step01-extract-cases.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts STF search result cards from case_query.htmlRaw into case_data documents.
Inputs: config/mongo.yaml, optional config/query.json, case_query records with htmlRaw.
Outputs: case_data records with identity metadata; updates case_query status pipeline.
Pipeline: claim case_query -> parse result cards -> upsert case_data -> update case_query status.
Dependencies: pymongo beautifulsoup4
------------------------------------------------------------

"""

from __future__ import annotations

import argparse
import json
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from bson import ObjectId
from bson.errors import InvalidId
from pymongo import ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from utils.mongo import get_mongo_client, load_yaml

# =============================================================================
# 0) PATHS CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
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


# =============================================================================
# 2) CONFIG LOADER
# =============================================================================

def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class MongoCfg:
    """
    Config consolidada do MongoDB + nomes de collections + statuses.
    """
    uri: str
    database: str
    case_query_collection: str
    case_data_collection: str
    status_input: str
    status_processing: str
    status_ok: str
    status_error: str


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    """
    Interpreta mongo.yaml.

    Estrutura suportada:

    {
    "mongo": {
        "uri": "...",
        "database": "...",
        "collections": {                    # opcional
        "case_query": "case_query",
        "case_data": "case_data"
        },
        "pipeline_status": {                # opcional
        "input": "new",
        "processing": "extracting",
        "ok": "extracted",
        "error": "error"
        }
    }
    }
    """
    m = raw.get("mongo", {}) or {}

    uri = str(m.get("uri") or "").strip()
    database = str(m.get("database") or "").strip()
    if not uri or not database:
        raise ValueError("mongo.yaml inválido: campos obrigatórios 'mongo.uri' e 'mongo.database'")

    collections = m.get("collections", {}) if isinstance(m.get("collections"), dict) else {}
    statuses = m.get("pipeline_status", {}) if isinstance(m.get("pipeline_status"), dict) else {}

    return MongoCfg(
        uri=uri,
        database=database,
        case_query_collection=str(collections.get("case_query") or "case_query"),
        case_data_collection=str(collections.get("case_data") or "case_data"),
        status_input=str(statuses.get("input") or "new"),
        status_processing=str(statuses.get("processing") or "extracting"),
        status_ok=str(statuses.get("ok") or "extracted"),
        status_error=str(statuses.get("error") or "error"),
    )


# =============================================================================
# 3) UTILS (limpeza / setters condicionais)
# =============================================================================

def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s == "N/A":
        return None
    return s


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _parse_br_date(value: Optional[str]) -> Optional[datetime]:
    s = _clean_str(value)
    if not s:
        return None
    try:
        dt = datetime.strptime(s, "%d/%m/%Y")
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc)


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


# =============================================================================
# 4) MONGO HELPERS (case_query -> case_data)
# =============================================================================

def get_collections(cfg: MongoCfg) -> Tuple[Collection, Collection]:
    log("INFO", "Conectando ao MongoDB...")
    client, db_name = get_mongo_client(MONGO_CONFIG_PATH)
    db = client[db_name]
    log("INFO", f"MongoDB conectado | db='{db_name}'")
    log("INFO", f"Collections | case_query='{cfg.case_query_collection}' | case_data='{cfg.case_data_collection}'")
    return db[cfg.case_query_collection], db[cfg.case_data_collection]


def claim_next_case_query(case_query_col: Collection, cfg: MongoCfg) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do próximo documento em case_query com status='new'.
    """
    log("INFO", f"Claim atômico | status='{cfg.status_input}' -> '{cfg.status_processing}'")
    return case_query_col.find_one_and_update(
        {"status": cfg.status_input},
        {"$set": {"status": cfg.status_processing, "extractingAt": utc_now()}},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_case_query_ok(case_query_col: Collection, doc_id, cfg: MongoCfg, *, extracted_count: int) -> None:
    case_query_col.update_one(
        {"_id": doc_id, "status": cfg.status_processing},
        {"$set": {
            "status": cfg.status_ok,
            "processedDate": utc_now(),
            "extractedCount": int(extracted_count),
        }},
    )


def mark_case_query_error(case_query_col: Collection, doc_id, cfg: MongoCfg, *, error_msg: str) -> None:
    case_query_col.update_one(
        {"_id": doc_id, "status": cfg.status_processing},
        {"$set": {
            "status": cfg.status_error,
            "processedDate": utc_now(),
            "error": _clean_ws(error_msg),
        }},
    )


# =============================================================================
# 5) EXTRAÇÃO DOS CARDS (result-container)
# =============================================================================

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


def _extract_date_by_regex(container, label_contains: str) -> Optional[datetime]:
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            m = re.search(r"\d{2}/\d{2}/\d{4}", txt)
            if m:
                return _parse_br_date(m.group(0))
            nxt = el.find_next("span")
            if nxt:
                return _parse_br_date(nxt.get_text(" ", strip=True))
    return None


def _extract_case_class(container, fallback_title: Optional[str]) -> Optional[str]:
    for a in container.find_all("a"):
        if a.has_attr("href") and "classe=" in a["href"]:
            parsed = urlparse(a["href"])
            qs = parse_qs(parsed.query)
            if "classe" in qs and qs["classe"]:
                return _clean_str(qs["classe"][0])
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
    texts = container.find_all(string=re.compile(keyword, re.IGNORECASE))
    for t in texts:
        m = re.search(r"\((\d+)\)", str(t))
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def _extract_dom_id(node) -> Optional[str]:
    if node and getattr(node, "attrs", None) and "id" in node.attrs:
        return _clean_str(node.attrs.get("id"))
    return None


def _derive_from_title(case_title: str) -> Dict[str, str]:
    out: Dict[str, str] = {}
    title = _clean_ws(case_title)
    if not title:
        return out

    out["caseCode"] = title

    m_class = re.match(r"^([A-Z]{2,})\b", title)
    if m_class:
        out["caseClassDetail"] = m_class.group(1)

    m_num = re.search(r"\b(\d[\d\.\-]*)\b", title)
    if m_num:
        out["caseNumberDetail"] = m_num.group(1)

    return out


def extract_cards(html_raw: str, source_case_query_id: str) -> List[Dict[str, Any]]:
    soup = BeautifulSoup(html_raw, "html.parser")
    containers = _find_result_containers(soup)

    log("INFO", f"Cards encontrados (result-container): {len(containers)}")

    out_docs: List[Dict[str, Any]] = []
    for idx, container in enumerate(containers, start=1):
        case_title = _extract_case_title(container)
        stf_id = _extract_stf_decision_id(container)
        case_url = _extract_case_url(container)

        if not stf_id:
            log("WARN", f"Card #{idx}: sem stfDecisionId (ignorado)")
            continue

        now = utc_now()
        doc: Dict[str, Any] = {}

        _set_if(doc, "caseTitle", case_title)

        # dates/
        judgment_date = _extract_date_by_regex(container, "Julgamento")
        publication_date = _extract_date_by_regex(container, "Publicação")
        _set_if(doc, "dates", _subdoc_if_any([
            ("judgmentDate", judgment_date),
            ("publicationDate", publication_date),
        ]))

        # caseContent/
        _set_if(doc, "caseContent", _subdoc_if_any([
            ("caseUrl", case_url),
        ]))

        # campos derivados do card — serão colocados dentro de case_data.identity
        judging_body = _extract_labeled_value(container, "Órgão julgador")
        rapporteur = _extract_labeled_value(container, "Relator")
        opinion_writer = _extract_labeled_value(container, "Redator")
        case_class = _extract_case_class(container, case_title)
        case_number = _extract_case_number(container, case_title)

        full_text_occ = _extract_occurrences(container, "Inteiro teor")
        indexing_occ = _extract_occurrences(container, "Indexação")
        dom_result_id = _extract_dom_id(container)

        dom_clip = None
        for b in container.find_all("button"):
            if b.has_attr("id") and b.has_attr("mattooltip"):
                tip = (b.get("mattooltip") or "").lower()
                if any(w in tip for w in ("copiar", "copy", "link")):
                    dom_clip = _clean_str(b["id"])
                    break

        # identity/ (enriquecida com campos do card)
        identity_pairs: List[Tuple[str, Any]] = [
            ("stfDecisionId", stf_id),
            ("caseQueryId", source_case_query_id),
        ]
        if case_title:
            derived = _derive_from_title(case_title)
            identity_pairs.append(("caseCode", derived.get("caseCode")))
            identity_pairs.append(("caseClassDetail", derived.get("caseClassDetail")))
            identity_pairs.append(("caseNumberDetail", derived.get("caseNumberDetail")))

        identity_pairs.extend([
            ("caseTitle", case_title),
            ("caseUrl", case_url),
            ("caseClass", case_class),
            ("caseNumber", case_number),
            ("judgingBody", judging_body),
            ("rapporteur", rapporteur),
            ("opinionWriter", opinion_writer),
            ("judgmentDate", judgment_date),
            ("publicationDate", publication_date),
            ("domResultContainerId", dom_result_id),
            ("domClipboardId", dom_clip),
        ])

        occ_sub: Dict[str, Any] = {}
        if isinstance(full_text_occ, int) and full_text_occ > 0:
            occ_sub["fullText"] = full_text_occ
        if isinstance(indexing_occ, int) and indexing_occ > 0:
            occ_sub["indexing"] = indexing_occ
        if occ_sub:
            identity_pairs.append(("occurrences", occ_sub))

        _set_if(doc, "identity", _subdoc_if_any(identity_pairs))

        # Também manter caseTitle no topo, se houver
        _set_if(doc, "caseTitle", case_title)

        # audit/
        audit: Dict[str, Any] = {}
        _set_if(audit, "extractionDate", now)
        _set_if(audit, "lastExtractedAt", now)
        _set_if(audit, "builtAt", now)
        _set_if(audit, "updatedAt", now)
        _set_if(audit, "sourceStatus", "extracted")
        _set_if(audit, "pipelineStatus", "extracted")
        _set_if(doc, "audit", audit)

        # processing/
        processing: Dict[str, Any] = {}
        _set_if(processing, "pipelineStatus", "extracted")
        _set_if(doc, "processing", processing)

        out_docs.append(doc)

    log("INFO", f"Docs gerados (case_data): {len(out_docs)}")
    return out_docs


def build_query_from_case_query(case_query_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Lê parâmetros de query do próprio documento em case_query.
    Esperado (geral):
    queryString, pageSize, inteiroTeor
    """
    return _subdoc_if_any([
        ("queryString", _clean_str(case_query_doc.get("queryString"))),
        ("pageSize", case_query_doc.get("pageSize")),
        ("inteiroTeor", case_query_doc.get("inteiroTeor")),
    ]) or {}


def upsert_case_data(case_col: Collection, *, doc: Dict[str, Any], stf_decision_id: str) -> Tuple[bool, Any]:
    """Upsert do documento em case_data.
    Retorna (created: bool, doc_id).
    """
    now = utc_now()
    audit = doc.get("audit") if isinstance(doc.get("audit"), dict) else {}
    audit["updatedAt"] = now
    audit["lastExtractedAt"] = now
    doc["audit"] = audit

    set_on_insert: Dict[str, Any] = {}
    if "builtAt" not in audit:
        set_on_insert["audit.builtAt"] = now

    res = case_col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": doc, "$setOnInsert": set_on_insert},
        upsert=True,
    )

    created = bool(res.upserted_id)
    doc_id = res.upserted_id
    if not doc_id:
        existing = case_col.find_one({"identity.stfDecisionId": stf_decision_id}, projection={"_id": 1})
        doc_id = existing.get("_id") if existing else None
    return created, doc_id


# =============================================================================
# 6) MAIN (processa todos os case_query com status=new)
# =============================================================================

def _process_case_query_doc(
    case_query_col: Collection,
    case_col: Collection,
    mongo_cfg: MongoCfg,
    case_query_doc: Dict[str, Any],
) -> int:
    doc_id = case_query_doc["_id"]
    doc_id_str = str(doc_id)
    log("INFO", f"Documento claimed | case_query._id={doc_id_str} | status='{mongo_cfg.status_processing}'")

    process_all = True
    try:
        step(4, 8, "Lendo HTML do case_query (campo htmlRaw)")
        html_raw = (case_query_doc.get("htmlRaw") or "").strip()
        if not html_raw:
            raise ValueError("Documento case_query não possui HTML (htmlRaw vazio).")

        log("INFO", f"HTML carregado | chars={len(html_raw)}")

        step(5, 8, "Extraindo dados de query do case_query (injetar em case_data.query)")
        query_sub = build_query_from_case_query(case_query_doc)
        if query_sub:
            log("INFO", f"Query detectada | {query_sub}")
        else:
            log("WARN", "Query não detectada no case_query (campo query não será incluído)")

        step(6, 8, "Extraindo cards do HTML")
        extracted_docs = extract_cards(html_raw, doc_id_str)

        if query_sub:
            for d in extracted_docs:
                _set_if(d, "query", query_sub)

        step(7, 8, "Persistindo decisões (UPSERT em case_data)")
        inserted = 0
        updated = 0
        skipped = 0
        for i, d in enumerate(extracted_docs, start=1):
            identity = d.get("identity") if isinstance(d.get("identity"), dict) else {}
            stf_id = _clean_str(identity.get("stfDecisionId"))
            if not stf_id:
                skipped += 1
                log("WARN", f"Persistência: doc #{i} sem stfDecisionId (ignorado)")
                continue

            exists = case_col.find_one({"identity.stfDecisionId": stf_id}, projection={"_id": 1})
            if not process_all and exists:
                skipped += 1
                log("INFO", f"Persistência: doc #{i} já existe (stfDecisionId={stf_id}) — ignorado (modo=new)")
                continue

            created, saved_doc_id = upsert_case_data(case_col, doc=d, stf_decision_id=stf_id)
            if created:
                inserted += 1
                log("INFO", f"Persistência: doc #{i} inserido | stfDecisionId={stf_id} | _id={saved_doc_id}")
            else:
                updated += 1
                log("INFO", f"Persistência: doc #{i} atualizado | stfDecisionId={stf_id} | _id={saved_doc_id}")

        log("INFO", f"Persistência concluída | inseridos={inserted} | atualizados={updated} | ignorados={skipped}")

        step(8, 8, "Atualizando status do case_query para 'extracted'")
        mark_case_query_ok(case_query_col, doc_id, mongo_cfg, extracted_count=len(extracted_docs))

        log("INFO", f"case_query._id={doc_id_str} | extractedCount={len(extracted_docs)} | status='{mongo_cfg.status_ok}'")
        return 0

    except Exception as e:
        step(8, 8, "Falha detectada: marcando case_query como 'error'")
        mark_case_query_error(case_query_col, doc_id, mongo_cfg, error_msg=str(e))

        log("ERROR", f"Erro ao processar case_query._id={doc_id_str}: {e}")
        log("ERROR", "Stacktrace completo:")
        print(traceback.format_exc())
        return 1

def main() -> int:
    parser = argparse.ArgumentParser(description="Extrai cards de case_query para case_data.")
    parser.add_argument("--case-query-id", dest="case_query_id", help="Processa apenas este _id do case_query.")
    args = parser.parse_args()

    total_steps = 8

    step(1, total_steps, "Carregando configurações (mongo.yaml / query.json)")
    mongo_raw = load_yaml(MONGO_CONFIG_PATH)
    mongo_cfg = build_mongo_cfg(mongo_raw)
    log("INFO", f"mongo.yaml OK | db='{mongo_cfg.database}'")

    try:
        _ = load_json(QUERY_CONFIG_PATH)
        log("INFO", f"query.json encontrado | path='{QUERY_CONFIG_PATH.resolve()}'")
    except FileNotFoundError:
        log("WARN", f"query.json não encontrado em {QUERY_CONFIG_PATH.resolve()} (ok para esta etapa)")

    step(2, total_steps, "Conectando ao MongoDB e obtendo collections")
    case_query_col, case_col = get_collections(mongo_cfg)

    if args.case_query_id:
        try:
            obj_id = ObjectId(args.case_query_id)
        except InvalidId:
            log("ERROR", "case_query_id inválido.")
            return 1

        case_query_doc = case_query_col.find_one({"_id": obj_id})
        if not case_query_doc:
            log("ERROR", "case_query não encontrado.")
            return 1

        case_query_col.update_one(
            {"_id": obj_id},
            {"$set": {"status": mongo_cfg.status_processing, "extractingAt": utc_now()}},
        )
        case_query_doc = case_query_col.find_one({"_id": obj_id}) or case_query_doc
        return _process_case_query_doc(case_query_col, case_col, mongo_cfg, case_query_doc)

    processed_total = 0
    while True:
        step(3, total_steps, f"Claim do próximo case_query (status='{mongo_cfg.status_input}')")
        case_query_doc = claim_next_case_query(case_query_col, mongo_cfg)
        if not case_query_doc:
            log("INFO", f"Nenhum documento com status='{mongo_cfg.status_input}' em '{mongo_cfg.case_query_collection}'.")
            break

        rc = _process_case_query_doc(case_query_col, case_col, mongo_cfg, case_query_doc)
        if rc == 0:
            processed_total += 1
        else:
            return 1

    log("INFO", f"Execução concluída | case_query processados={processed_total}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PyMongoError as e:
        log("ERROR", f"Erro MongoDB: {e}")
        sys.exit(2)
