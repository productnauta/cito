#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: app.py
Version: poc-v-d33      Date: 2026-02-01
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Flask web interface for doctrine citation search and drill-down over case_data.
Inputs: config/mongo.yaml, case_data collection.
Outputs: HTML pages with filters, aggregates, and case detail lists.
Dependencies: flask, pymongo, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import json
import os
import re
import sys
import tempfile
from uuid import uuid4
import subprocess
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from urllib.parse import parse_qs, urlparse

from flask import Flask, redirect, render_template, request, url_for
from bson import ObjectId
from bson.errors import InvalidId
from pymongo.collection import Collection
import yaml


BASE_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = BASE_DIR / "core"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))

from utils.mongo import get_case_data_collection, get_mongo_client
from utils.normalize import normalize_minister_name


CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
COLLECTION_NAME = "case_data"
DOCTRINE_PATH = "caseData.doctrineReferences"
DECISION_DETAILS_PATH = "caseData.decisionDetails"
MINISTER_VOTES_PATH = f"{DECISION_DETAILS_PATH}.ministerVotes"
DECISION_RESULT_PATH = f"{DECISION_DETAILS_PATH}.decisionResult.finalDecision"
CITATIONS_PATH = f"{DECISION_DETAILS_PATH}.citations"
KEYWORDS_PATH = "caseData.caseKeywords"
LEGISLATION_PATH = "caseData.legislationReferences"
CASE_QUERY_COLLECTION = "case_query"
SCRAPE_JOBS_COLLECTION = "scrape_jobs"
QUERY_CONFIG_PATH = CONFIG_DIR / "query.yaml"
PIPELINE_CONFIG_PATH = CONFIG_DIR / "pipeline.yaml"
PIPELINE_JOBS_COLLECTION = "pipeline_jobs"

MINISTER_DETAIL_LABELS = {
    "summary_analytics": "Analítico",
    "total_processes": "Total de processos",
    "total_relatorias": "Total de relatorias",
    "citations_made": "Citações Feitas",
    "doutrinas": "Doutrinas",
    "acordaos": "Acórdãos",
    "legislacoes": "Legislações",
    "decisoes": "Decisões",
    "relatoria_processos": "Relatoria dos Processos",
    "th_autor": "Autor",
    "th_total": "Total",
    "th_acordao": "Acórdão",
    "th_legislacao": "Legislação",
    "th_decisao": "Decisão",
    "th_titulo_caso": "Título do caso",
    "th_processo": "Processo",
}

WEB_LOG_DIR = BASE_DIR / "core" / "logs"
_WEB_LOG_STAMP = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
WEB_LOG_FILE = WEB_LOG_DIR / f"{_WEB_LOG_STAMP}-web-actions.log"

_collection: Optional[Collection] = None
_db = None


def _get_collection() -> Collection:
    global _collection
    if _collection is None:
        _collection = get_case_data_collection(MONGO_CONFIG_PATH, COLLECTION_NAME)
    return _collection


def _get_db():
    global _db
    if _db is None:
        client, db_name = get_mongo_client(MONGO_CONFIG_PATH)
        _db = client[db_name]
    return _db


def _get_case_query_collection() -> Collection:
    return _get_db()[CASE_QUERY_COLLECTION]


def _get_scrape_jobs_collection() -> Collection:
    return _get_db()[SCRAPE_JOBS_COLLECTION]


def _get_pipeline_jobs_collection() -> Collection:
    return _get_db()[PIPELINE_JOBS_COLLECTION]


def _regex(value: str, exact: bool = False) -> Dict[str, Any]:
    pattern = f"^{re.escape(value)}$" if exact else re.escape(value)
    return {"$regex": pattern, "$options": "i"}


def _year_range(year: int) -> Tuple[datetime, datetime]:
    return datetime(year, 1, 1), datetime(year + 1, 1, 1)


def _format_date(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d")
    if isinstance(value, date):
        return value.strftime("%Y-%m-%d")
    return str(value) if value else ""


def _parse_date_value(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _format_datetime(value: Any) -> str:
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value) if value else ""


def _parse_datetime_local(value: str) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%dT%H:%M")
    except ValueError:
        return None


def _normalize_decision_label(value: Optional[str]) -> str:
    if value is None:
        return "Não identificado"
    s = str(value).strip()
    if not s:
        return "Não identificado"
    low = s.lower()
    if "parcial" in low:
        return "Parcialmente indeferido"
    if any(tok in low for tok in ["defer", "procedente", "favoravel", "favorável"]):
        return "Deferido"
    if any(tok in low for tok in ["indefer", "improcedente", "contrario", "contrário"]):
        return "Indeferido"
    return "Outros"


def _normalize_vote_label(value: Optional[str]) -> str:
    if value is None:
        return "Outros"
    s = str(value).strip()
    if not s:
        return "Outros"
    return _normalize_decision_label(s)


def _as_bool(value: Any, default: bool = False) -> bool:
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


def _load_query_defaults() -> Dict[str, Any]:
    if not QUERY_CONFIG_PATH.exists():
        return {}
    raw = yaml.safe_load(QUERY_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    q = raw.get("query") if isinstance(raw.get("query"), dict) else {}
    paging = q.get("paging") if isinstance(q.get("paging"), dict) else {}
    sorting = q.get("sorting") if isinstance(q.get("sorting"), dict) else {}
    http = raw.get("http") if isinstance(raw.get("http"), dict) else {}
    runtime = raw.get("runtime") if isinstance(raw.get("runtime"), dict) else {}
    fixed = raw.get("fixed_query_params") if isinstance(raw.get("fixed_query_params"), dict) else {}
    flags = fixed.get("text_search_flags") if isinstance(fixed.get("text_search_flags"), dict) else {}
    filters = fixed.get("filters") if isinstance(fixed.get("filters"), dict) else {}

    return {
        "query_string": str(q.get("query_string") or q.get("search_term") or "").strip(),
        "full_text": _as_bool(q.get("full_text"), True),
        "page": int(paging.get("page") or 1),
        "page_size": int(paging.get("page_size") or 50),
        "sort": str(sorting.get("sort") or sorting.get("field") or "_score"),
        "sort_by": str(sorting.get("sort_by") or sorting.get("order") or "desc"),
        "request_delay_seconds": float(http.get("request_delay_seconds") or 0),
        "ssl_verify": _as_bool(http.get("ssl_verify"), True),
        "headed_mode": _as_bool(runtime.get("headed_mode"), False),
        "base": str(fixed.get("base") or "acordaos"),
        "synonym": _as_bool(flags.get("synonym"), True),
        "plural": _as_bool(flags.get("plural"), True),
        "stems": _as_bool(flags.get("stems"), False),
        "exact_search": _as_bool(flags.get("exact_search"), True),
        "process_class_sigla": list(filters.get("process_class_sigla") or []),
        "date_start": "",
        "date_end": "",
    }


def _load_query_raw() -> Dict[str, Any]:
    if not QUERY_CONFIG_PATH.exists():
        return {}
    return yaml.safe_load(QUERY_CONFIG_PATH.read_text(encoding="utf-8")) or {}


def _merge_query_cfg(raw: Dict[str, Any], job_query: Dict[str, Any]) -> Dict[str, Any]:
    q = raw.setdefault("query", {})
    paging = q.setdefault("paging", {})
    sorting = q.setdefault("sorting", {})
    http = raw.setdefault("http", {})
    runtime = raw.setdefault("runtime", {})
    fixed = raw.setdefault("fixed_query_params", {})
    flags = fixed.setdefault("text_search_flags", {})
    filters = fixed.setdefault("filters", {})

    q["query_string"] = job_query.get("queryString", q.get("query_string"))
    q["full_text"] = job_query.get("fullText", q.get("full_text", True))
    paging["page"] = job_query.get("page", paging.get("page", 1))
    paging["page_size"] = job_query.get("pageSize", paging.get("page_size", 50))
    sorting["sort"] = job_query.get("sort", sorting.get("sort", "_score"))
    sorting["sort_by"] = job_query.get("sortBy", sorting.get("sort_by", "desc"))
    http["request_delay_seconds"] = job_query.get("requestDelaySeconds", http.get("request_delay_seconds", 0))
    http["ssl_verify"] = job_query.get("sslVerify", http.get("ssl_verify", True))
    runtime["headed_mode"] = job_query.get("headedMode", runtime.get("headed_mode", False))
    fixed["base"] = job_query.get("base", fixed.get("base", "acordaos"))
    flags["synonym"] = job_query.get("synonym", flags.get("synonym", True))
    flags["plural"] = job_query.get("plural", flags.get("plural", True))
    flags["stems"] = job_query.get("stems", flags.get("stems", False))
    flags["exact_search"] = job_query.get("exactSearch", flags.get("exact_search", True))
    filters["process_class_sigla"] = job_query.get(
        "processClassSigla", filters.get("process_class_sigla", [])
    )
    return raw


def _load_pipeline_steps() -> List[Dict[str, Any]]:
    if not PIPELINE_CONFIG_PATH.exists():
        return []
    raw = yaml.safe_load(PIPELINE_CONFIG_PATH.read_text(encoding="utf-8")) or {}
    pipeline = raw.get("pipeline") if isinstance(raw.get("pipeline"), dict) else {}
    execution = pipeline.get("execution") if isinstance(pipeline.get("execution"), dict) else {}
    steps = execution.get("steps") if isinstance(execution.get("steps"), list) else []
    output: List[Dict[str, Any]] = []
    for step in steps:
        if not isinstance(step, dict):
            continue
        if step.get("enabled") is False:
            continue
        script = str(step.get("script") or "").strip()
        if not script:
            continue
        output.append(
            {
                "script": script,
                "input_format": str(step.get("input_format") or ""),
            }
        )
    return output


def _parse_query_url(query_url: str) -> Dict[str, Any]:
    if not query_url:
        return {}
    parsed = urlparse(query_url)
    qs = parse_qs(parsed.query)
    return {
        "query_string": (qs.get("queryString") or [""])[0],
        "full_text": (qs.get("pesquisa_inteiro_teor") or [""])[0],
        "page": (qs.get("page") or [""])[0],
        "page_size": (qs.get("pageSize") or [""])[0],
        "sort": (qs.get("sort") or [""])[0],
        "sort_by": (qs.get("sortBy") or [""])[0],
        "base": (qs.get("base") or [""])[0],
        "synonym": (qs.get("sinonimo") or [""])[0],
        "plural": (qs.get("plural") or [""])[0],
        "stems": (qs.get("radicais") or [""])[0],
        "exact_search": (qs.get("buscaExata") or [""])[0],
        "process_class_sigla": qs.get("processo_classe_processual_unificada_classe_sigla") or [],
    }


def _status_meta() -> Dict[str, Dict[str, str]]:
    return {
        "scheduled": {"label": "Agendado", "class": "status-pill status-pill--scheduled"},
        "running": {"label": "Em andamento", "class": "status-pill status-pill--running"},
        "completed": {"label": "Concluído", "class": "status-pill status-pill--success"},
        "failed": {"label": "Falhou", "class": "status-pill status-pill--danger"},
        "canceled": {"label": "Cancelado", "class": "status-pill status-pill--muted"},
        "skipped": {"label": "Ignorado", "class": "status-pill status-pill--muted"},
        "extracted": {"label": "Concluído", "class": "status-pill status-pill--success"},
        "extracting": {"label": "Em andamento", "class": "status-pill status-pill--running"},
        "new": {"label": "Agendado", "class": "status-pill status-pill--scheduled"},
        "error": {"label": "Falhou", "class": "status-pill status-pill--danger"},
        "unknown": {"label": "Desconhecido", "class": "status-pill status-pill--muted"},
    }


def _web_log(action: str, payload: Dict[str, Any]) -> None:
    try:
        WEB_LOG_DIR.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        entry = {
            "ts": stamp,
            "action": action,
            "payload": payload,
        }
        with WEB_LOG_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    except Exception:
        # Evita quebrar o app em caso de falha de escrita
        pass


def _get_filters(args: Dict[str, Any]) -> Dict[str, str]:
    return {
        "author": str(args.get("author") or "").strip(),
        "title": str(args.get("title") or "").strip(),
        "case_class": str(args.get("case_class") or "").strip(),
        "judgment_year": str(args.get("judgment_year") or "").strip(),
        "rapporteur": str(args.get("rapporteur") or "").strip(),
        "judging_body": str(args.get("judging_body") or "").strip(),
        "minister": str(args.get("minister") or "").strip(),
    }


def _get_process_filters(args: Dict[str, Any]) -> Dict[str, str]:
    return {
        "title": str(args.get("title") or "").strip(),
        "case_class": str(args.get("case_class") or "").strip(),
        "rapporteur": str(args.get("rapporteur") or "").strip(),
        "date_start": str(args.get("date_start") or "").strip(),
        "date_end": str(args.get("date_end") or "").strip(),
        "author": str(args.get("author") or "").strip(),
        "legislation_norm": str(args.get("legislation_norm") or "").strip(),
        "acordao_ref": str(args.get("acordao_ref") or "").strip(),
    }


def _get_ministro_filters(args: Dict[str, Any]) -> Dict[str, str]:
    return {
        "minister": str(args.get("minister") or "").strip(),
        "case_class": str(args.get("case_class") or "").strip(),
        "date_start": str(args.get("date_start") or "").strip(),
        "date_end": str(args.get("date_end") or "").strip(),
        "process": str(args.get("process") or "").strip(),
    }


def _build_match(filters: Dict[str, str], overrides: Optional[Dict[str, Tuple[str, bool]]] = None) -> Dict[str, Any]:
    overrides = overrides or {}
    and_clauses: List[Dict[str, Any]] = []

    def _resolve(key: str) -> Tuple[str, bool]:
        if key in overrides:
            return overrides[key][0], overrides[key][1]
        return filters.get(key, ""), False

    author_value, author_exact = _resolve("author")
    title_value, title_exact = _resolve("title")
    rapporteur_value, rapporteur_exact = _resolve("rapporteur")
    minister_value, minister_exact = _resolve("minister")

    if rapporteur_value:
        rapporteur_value = normalize_minister_name(rapporteur_value) or rapporteur_value
    if minister_value:
        minister_value = normalize_minister_name(minister_value) or minister_value

    case_class = filters.get("case_class") or ""
    judging_body = filters.get("judging_body") or ""
    year_raw = filters.get("judgment_year") or ""

    if case_class:
        rx = _regex(case_class)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseClass": rx},
                    {"caseIdentification.caseClass": rx},
                ]
            }
        )

    if rapporteur_value:
        rx = _regex(rapporteur_value, exact=rapporteur_exact)
        and_clauses.append(
            {
                "$or": [
                    {"identity.rapporteur": rx},
                    {"caseIdentification.rapporteur": rx},
                ]
            }
        )

    if minister_value:
        rx = _regex(minister_value, exact=minister_exact)
        and_clauses.append(
            {
                "$or": [
                    {"identity.rapporteur": rx},
                    {"caseIdentification.rapporteur": rx},
                    {MINISTER_VOTES_PATH: {"$elemMatch": {"ministerName": rx}}},
                ]
            }
        )

    if judging_body:
        rx = _regex(judging_body)
        and_clauses.append(
            {
                "$or": [
                    {"identity.judgingBody": rx},
                    {"caseIdentification.judgingBody": rx},
                ]
            }
        )

    if year_raw.isdigit():
        year = int(year_raw)
        start, end = _year_range(year)
        and_clauses.append({"dates.judgmentDate": {"$gte": start, "$lt": end}})

    elem: Dict[str, Any] = {}
    if author_value:
        elem["author"] = _regex(author_value, exact=author_exact)
    if title_value:
        elem["publicationTitle"] = _regex(title_value, exact=title_exact)
    if elem:
        and_clauses.append({DOCTRINE_PATH: {"$elemMatch": elem}})
    elif minister_value:
        and_clauses.append({DOCTRINE_PATH: {"$exists": True, "$ne": []}})

    if not and_clauses:
        return {}
    return {"$and": and_clauses}


def _build_ministro_case_match(filters: Dict[str, str]) -> Dict[str, Any]:
    and_clauses: List[Dict[str, Any]] = []
    case_class = filters.get("case_class") or ""
    process_value = filters.get("process") or ""

    if case_class:
        rx = _regex(case_class)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseClass": rx},
                    {"caseIdentification.caseClass": rx},
                ]
            }
        )

    if process_value:
        rx = _regex(process_value)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseNumber": rx},
                    {"identity.caseTitle": rx},
                    {"identity.stfDecisionId": rx},
                    {"caseIdentification.caseNumber": rx},
                ]
            }
        )

    start_date = _parse_date_value(filters.get("date_start") or "")
    end_date = _parse_date_value(filters.get("date_end") or "")
    if start_date or end_date:
        date_match: Dict[str, Any] = {}
        if start_date:
            date_match["$gte"] = datetime.combine(start_date, datetime.min.time())
        if end_date:
            date_match["$lt"] = datetime.combine(end_date + timedelta(days=1), datetime.min.time())
        and_clauses.append({"dates.judgmentDate": date_match})

    if not and_clauses:
        return {}
    return {"$and": and_clauses}


def _case_id_expr() -> Dict[str, Any]:
    return {"$ifNull": ["$identity.stfDecisionId", "$_id"]}


def _aggregate_authors(collection: Collection, filters: Dict[str, str]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    base_match = _build_match(filters)
    if base_match:
        pipeline.append({"$match": base_match})

    pipeline.append({"$unwind": f"${DOCTRINE_PATH}"})

    citation_match: Dict[str, Any] = {}
    if filters.get("author"):
        citation_match[f"{DOCTRINE_PATH}.author"] = _regex(filters["author"])
    if filters.get("title"):
        citation_match[f"{DOCTRINE_PATH}.publicationTitle"] = _regex(filters["title"])
    if citation_match:
        pipeline.append({"$match": citation_match})

    pipeline.append({"$match": {f"{DOCTRINE_PATH}.author": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": f"${DOCTRINE_PATH}.author",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    return list(collection.aggregate(pipeline))


def _aggregate_titles(collection: Collection, filters: Dict[str, str]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    base_match = _build_match(filters)
    if base_match:
        pipeline.append({"$match": base_match})

    pipeline.append({"$unwind": f"${DOCTRINE_PATH}"})

    citation_match: Dict[str, Any] = {}
    if filters.get("author"):
        citation_match[f"{DOCTRINE_PATH}.author"] = _regex(filters["author"])
    if filters.get("title"):
        citation_match[f"{DOCTRINE_PATH}.publicationTitle"] = _regex(filters["title"])
    if citation_match:
        pipeline.append({"$match": citation_match})

    pipeline.append({"$match": {f"{DOCTRINE_PATH}.publicationTitle": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": f"${DOCTRINE_PATH}.publicationTitle",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    return list(collection.aggregate(pipeline))


def _aggregate_rapporteurs(collection: Collection, filters: Dict[str, str]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    base_match = _build_match(filters)
    if base_match:
        pipeline.append({"$match": base_match})

    pipeline.append(
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                }
            }
        }
    )
    pipeline.append({"$match": {"_rapporteur": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": "$_rapporteur",
                "cases": {"$addToSet": _case_id_expr()},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": {"$size": "$cases"}}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    return list(collection.aggregate(pipeline))


def _count_cases(collection: Collection, filters: Dict[str, str]) -> int:
    pipeline: List[Dict[str, Any]] = []
    base_match = _build_match(filters)
    if base_match:
        pipeline.append({"$match": base_match})
    pipeline.append({"$group": {"_id": _case_id_expr()}})
    pipeline.append({"$count": "total"})
    result = list(collection.aggregate(pipeline))
    return int(result[0]["total"]) if result else 0


def _fetch_cases(collection: Collection, match: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
    projection = {
        "identity.stfDecisionId": 1,
        "identity.caseTitle": 1,
        "identity.rapporteur": 1,
        "identity.judgingBody": 1,
        "identity.caseUrl": 1,
        "caseIdentification.rapporteur": 1,
        "caseIdentification.judgingBody": 1,
        "caseTitle": 1,
        "caseContent.caseUrl": 1,
        "dates.judgmentDate": 1,
        DECISION_RESULT_PATH: 1,
        MINISTER_VOTES_PATH: 1,
    }
    cursor = collection.find(match, projection=projection).sort("dates.judgmentDate", -1)
    if limit:
        cursor = cursor.limit(limit)
    cases: List[Dict[str, Any]] = []
    for doc in cursor:
        identity = doc.get("identity") or {}
        case_ident = doc.get("caseIdentification") or {}
        dates = doc.get("dates") or {}
        case_content = doc.get("caseContent") or {}
        decision_details = (doc.get("caseData") or {}).get("decisionDetails") or {}

        case_title = identity.get("caseTitle") or doc.get("caseTitle") or "-"
        stf_id = identity.get("stfDecisionId") or str(doc.get("_id"))
        rapporteur = identity.get("rapporteur") or case_ident.get("rapporteur") or "-"
        judging_body = identity.get("judgingBody") or case_ident.get("judgingBody") or "-"
        case_url = case_content.get("caseUrl") or identity.get("caseUrl") or ""
        judgment_date = _format_date(dates.get("judgmentDate"))
        decision_final = (
            (decision_details.get("decisionResult") or {}).get("finalDecision") or "—"
        )
        minister_votes = decision_details.get("ministerVotes") or []
        vote_type = "—"
        if rapporteur != "-" and minister_votes:
            rapporteur_key = str(rapporteur).strip().lower()
            for entry in minister_votes:
                minister_name = str(entry.get("ministerName") or "").strip().lower()
                if minister_name and minister_name == rapporteur_key:
                    vote_type = entry.get("voteType") or "—"
                    break

        cases.append(
            {
                "case_title": case_title,
                "stf_id": stf_id,
                "case_url": case_url,
                "judgment_date": judgment_date,
                "judging_body": judging_body,
                "decision_final": decision_final,
                "vote_type": vote_type,
            }
        )
    return cases


def _limit_value(raw: Any, default: int = 10) -> int:
    try:
        value = int(raw)
        return value if value > 0 else default
    except (TypeError, ValueError):
        return default


def _citations_count_expr(allowed_types: List[str]) -> Dict[str, Any]:
    return {
        "$size": {
            "$filter": {
                "input": {"$ifNull": [f"${CITATIONS_PATH}", []]},
                "as": "c",
                "cond": {"$in": ["$$c.citationType", allowed_types]},
            }
        }
    }


def _aggregate_cases_by_year(collection: Collection, match: Dict[str, Any]) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$match": {"dates.judgmentDate": {"$type": "date"}}})
    pipeline.append({"$group": {"_id": {"$year": "$dates.judgmentDate"}, "total": {"$sum": 1}}})
    pipeline.append({"$project": {"_id": 0, "year": "$_id", "total": 1}})
    pipeline.append({"$sort": {"year": 1}})
    return list(collection.aggregate(pipeline))


def _calculate_cases_per_year_avg(year_counts: List[Dict[str, Any]]) -> Optional[float]:
    if not year_counts:
        return None
    total = sum(int(item.get("total") or 0) for item in year_counts)
    return total / max(len(year_counts), 1)


def _calculate_case_trend(year_counts: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if len(year_counts) < 2:
        return None
    prev = int(year_counts[-2].get("total") or 0)
    last = int(year_counts[-1].get("total") or 0)
    if prev == 0:
        return None
    change = ((last - prev) / prev) * 100
    return {"year": year_counts[-1].get("year"), "change": change}


def _aggregate_decision_distribution(
    collection: Collection, match: Dict[str, Any]
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append(
        {
            "$addFields": {
                "_finalDecision": {
                    "$ifNull": [f"${DECISION_RESULT_PATH}", "Não informado"]
                }
            }
        }
    )
    pipeline.append({"$group": {"_id": "$_finalDecision", "total": {"$sum": 1}}})
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    return list(collection.aggregate(pipeline))


def _aggregate_vote_vencido_rate(collection: Collection, match: Dict[str, Any]) -> Optional[float]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${MINISTER_VOTES_PATH}"})
    pipeline.append(
        {
            "$addFields": {
                "_voteType": {"$ifNull": [f"${MINISTER_VOTES_PATH}.voteType", ""]}
            }
        }
    )
    pipeline.append(
        {
            "$group": {
                "_id": None,
                "total_defined": {
                    "$sum": {"$cond": [{"$ne": ["$_voteType", ""]}, 1, 0]}
                },
                "total_vencido": {
                    "$sum": {"$cond": [{"$eq": ["$_voteType", "vencido"]}, 1, 0]}
                },
            }
        }
    )
    result = list(collection.aggregate(pipeline))
    if not result:
        return None
    total_defined = int(result[0].get("total_defined") or 0)
    total_vencido = int(result[0].get("total_vencido") or 0)
    if total_defined == 0:
        return None
    return (total_vencido / total_defined) * 100


def _aggregate_avg_citations(
    collection: Collection, match: Dict[str, Any], allowed_types: List[str]
) -> Optional[float]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append(
        {
            "$addFields": {
                "_citationsCount": _citations_count_expr(allowed_types),
                "_hasCitations": {"$cond": [{"$isArray": f"${CITATIONS_PATH}"}, 1, 0]},
            }
        }
    )
    pipeline.append(
        {
            "$group": {
                "_id": None,
                "avg": {"$avg": "$_citationsCount"},
                "cases_with_citations": {"$sum": "$_hasCitations"},
            }
        }
    )
    result = list(collection.aggregate(pipeline))
    if not result:
        return None
    cases_with_citations = int(result[0].get("cases_with_citations") or 0)
    if cases_with_citations == 0:
        return None
    return float(result[0].get("avg") or 0.0)


def _aggregate_citation_ratio(
    collection: Collection, match: Dict[str, Any]
) -> Optional[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${CITATIONS_PATH}"})
    pipeline.append({"$group": {"_id": f"${CITATIONS_PATH}.citationType", "total": {"$sum": 1}}})
    result = list(collection.aggregate(pipeline))
    if not result:
        return None
    counts = {row["_id"]: int(row.get("total") or 0) for row in result if row.get("_id")}
    doutrina = counts.get("doutrina", 0)
    legislacao = counts.get("legislacao", 0)
    total = doutrina + legislacao
    if total == 0:
        return None
    return {
        "doutrina": doutrina,
        "legislacao": legislacao,
        "percent_doutrina": (doutrina / total) * 100,
        "percent_legislacao": (legislacao / total) * 100,
    }


def _aggregate_top_doctrine_titles(
    collection: Collection, match: Dict[str, Any], limit: int = 3
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${DOCTRINE_PATH}"})
    pipeline.append({"$match": {f"{DOCTRINE_PATH}.publicationTitle": {"$nin": [None, ""]}}})
    pipeline.append({"$group": {"_id": f"${DOCTRINE_PATH}.publicationTitle", "total": {"$sum": 1}}})
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    return list(collection.aggregate(pipeline))


def _build_process_match(filters: Dict[str, str]) -> Dict[str, Any]:
    and_clauses: List[Dict[str, Any]] = []

    title_value = filters.get("title") or ""
    case_class = filters.get("case_class") or ""
    rapporteur = filters.get("rapporteur") or ""
    if rapporteur:
        rapporteur = normalize_minister_name(rapporteur) or rapporteur
    author = filters.get("author") or ""
    legislation_norm = filters.get("legislation_norm") or ""
    acordao_ref = filters.get("acordao_ref") or ""
    date_start = _parse_date_value(filters.get("date_start") or "")
    date_end = _parse_date_value(filters.get("date_end") or "")

    if title_value:
        rx = _regex(title_value)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseTitle": rx},
                    {"caseTitle": rx},
                ]
            }
        )

    if case_class:
        rx = _regex(case_class, exact=True)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseClass": rx},
                    {"caseIdentification.caseClass": rx},
                ]
            }
        )

    if rapporteur:
        rx = _regex(rapporteur, exact=True)
        and_clauses.append(
            {
                "$or": [
                    {"identity.rapporteur": rx},
                    {"caseIdentification.rapporteur": rx},
                ]
            }
        )

    if author:
        and_clauses.append(
            {DOCTRINE_PATH: {"$elemMatch": {"author": _regex(author)}}}
        )

    if legislation_norm:
        rx = _regex(legislation_norm)
        and_clauses.append(
            {LEGISLATION_PATH: {"$elemMatch": {"normIdentifier": rx}}}
        )

    if acordao_ref:
        rx = _regex(acordao_ref)
        and_clauses.append(
            {
                "caseData.notesReferences": {
                    "$elemMatch": {
                        "noteType": "stf_acordao",
                        "$or": [
                            {"rawLine": rx},
                            {"items": {"$elemMatch": {"rawRef": rx}}},
                        ],
                    }
                }
            }
        )

    if date_start or date_end:
        date_match: Dict[str, Any] = {}
        if date_start:
            date_match["$gte"] = datetime.combine(date_start, datetime.min.time())
        if date_end:
            date_match["$lt"] = datetime.combine(date_end + timedelta(days=1), datetime.min.time())
        and_clauses.append({"dates.judgmentDate": date_match})

    if not and_clauses:
        return {}
    return {"$and": and_clauses}


def _fetch_processes(collection: Collection, match: Dict[str, Any], limit: int = 25) -> List[Dict[str, Any]]:
    projection = {
        "identity.stfDecisionId": 1,
        "identity.caseTitle": 1,
        "identity.caseClass": 1,
        "identity.rapporteur": 1,
        "caseTitle": 1,
        "caseIdentification.caseClass": 1,
        "caseIdentification.rapporteur": 1,
        "dates.judgmentDate": 1,
        DOCTRINE_PATH: 1,
        KEYWORDS_PATH: 1,
        LEGISLATION_PATH: 1,
        "caseData.caseParties": 1,
    }
    cursor = collection.find(match, projection=projection).sort("dates.judgmentDate", -1)
    if limit:
        cursor = cursor.limit(limit)

    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        identity = doc.get("identity") or {}
        case_ident = doc.get("caseIdentification") or {}
        case_title = identity.get("caseTitle") or doc.get("caseTitle") or "-"
        case_class = identity.get("caseClass") or case_ident.get("caseClass") or "-"
        rapporteur = identity.get("rapporteur") or case_ident.get("rapporteur") or "-"
        judgment_date = _format_date((doc.get("dates") or {}).get("judgmentDate"))
        case_data = doc.get("caseData") or {}
        doctrine_count = len(case_data.get("doctrineReferences") or [])
        keywords_count = len(case_data.get("caseKeywords") or [])
        legislation_count = len(case_data.get("legislationReferences") or [])
        parties_count = len(case_data.get("caseParties") or [])
        stf_id = identity.get("stfDecisionId") or str(doc.get("_id"))

        rows.append(
            {
                "case_title": case_title,
                "case_class": case_class,
                "rapporteur": rapporteur,
                "judgment_date": judgment_date,
                "doctrine_count": doctrine_count,
                "keywords_count": keywords_count,
                "legislation_count": legislation_count,
                "parties_count": parties_count,
                "stf_id": stf_id,
            }
        )
    return rows


def _tag_size_class(value: int, max_value: int) -> str:
    if max_value <= 0:
        return "tag-pill--sm"
    ratio = value / max_value
    if ratio >= 0.8:
        return "tag-pill--lg"
    if ratio >= 0.5:
        return "tag-pill--md"
    return "tag-pill--sm"


def _aggregate_doctrine_tag_cloud(
    collection: Collection, match: Dict[str, Any], limit: int
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${DOCTRINE_PATH}"})
    pipeline.append({"$match": {f"{DOCTRINE_PATH}.author": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": f"${DOCTRINE_PATH}.author",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    rows = list(collection.aggregate(pipeline))
    max_total = max((row.get("total", 0) for row in rows), default=0)
    for row in rows:
        row["size_class"] = _tag_size_class(int(row.get("total", 0)), max_total)
    return rows


def _aggregate_legislation_tag_cloud(
    collection: Collection, match: Dict[str, Any], limit: int
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${LEGISLATION_PATH}"})
    pipeline.append(
        {"$addFields": {"_normId": f"${LEGISLATION_PATH}.normIdentifier"}}
    )
    pipeline.append({"$match": {"_normId": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": "$_normId",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    rows = list(collection.aggregate(pipeline))
    max_total = max((row.get("total", 0) for row in rows), default=0)
    for row in rows:
        row["size_class"] = _tag_size_class(int(row.get("total", 0)), max_total)
    return rows


def _aggregate_acordao_tag_cloud(
    collection: Collection, match: Dict[str, Any], limit: int
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": "$caseData.notesReferences"})
    pipeline.append({"$match": {"caseData.notesReferences.noteType": "stf_acordao"}})
    pipeline.append({"$unwind": "$caseData.notesReferences.items"})
    pipeline.append(
        {
            "$addFields": {
                "_rawRef": {
                    "$trim": {"input": "$caseData.notesReferences.items.rawRef"}
                }
            }
        }
    )
    pipeline.append({"$match": {"_rawRef": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": "$_rawRef",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": 1}})
    pipeline.append({"$sort": {"total": -1, "label": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    rows = list(collection.aggregate(pipeline))
    max_total = max((row.get("total", 0) for row in rows), default=0)
    for row in rows:
        row["size_class"] = _tag_size_class(int(row.get("total", 0)), max_total)
    return rows


def _fetch_process_detail(collection: Collection, process_id: str) -> Optional[Dict[str, Any]]:
    query: Dict[str, Any] = {}
    try:
        query = {"_id": ObjectId(process_id)}
    except InvalidId:
        query = {"identity.stfDecisionId": process_id}

    doc = collection.find_one(query)
    if not doc:
        return None

    identity = doc.get("identity") or {}
    case_ident = doc.get("caseIdentification") or {}
    dates = doc.get("dates") or {}
    case_content = doc.get("caseContent") or {}
    case_data = doc.get("caseData") or {}
    decision_details = case_data.get("decisionDetails") or {}

    doctrine_refs = case_data.get("doctrineReferences") or []
    legislation_refs_raw = case_data.get("legislationReferences") or []
    parties_raw = case_data.get("caseParties") or []
    parties = []
    for party in parties_raw:
        if isinstance(party, dict):
            party_type = (
                party.get("partieType")
                or party.get("partyType")
                or party.get("type")
                or party.get("role")
                or "-"
            )
            party_name = (
                party.get("partieName")
                or party.get("partyName")
                or party.get("name")
                or "-"
            )
        else:
            party_type = "-"
            party_name = str(party) if party is not None else "-"
        if not party_type:
            party_type = "-"
        if not party_name:
            party_name = "-"
        parties.append({"type": party_type, "name": party_name})
    legislation_refs = []
    legislation_ids = []
    legislation_refs_total = 0
    for norm in legislation_refs_raw:
        if not isinstance(norm, dict):
            norm_id = str(norm)
            legislation_refs.append({"normIdentifier": norm_id})
            legislation_ids.append(norm_id)
            if norm_id:
                legislation_refs_total += 1
            continue
        norm_id = norm.get("normIdentifier")
        norm_id_str = str(norm_id).strip() if norm_id is not None else ""
        if not norm_id_str or norm_id_str.isdigit():
            norm_type = str(norm.get("normType") or "NORMA").strip()
            norm_year = norm.get("normYear")
            if norm_year:
                norm_id_str = f"{norm_type}-{norm_year}"
            else:
                norm_desc = str(norm.get("normDescription") or "").strip()
                norm_id_str = norm_desc or norm_id_str or "—"
        legislation_refs.append({"normIdentifier": norm_id_str})
        legislation_ids.append(norm_id_str)
        refs = norm.get("normReferences")
        if isinstance(refs, list) and refs:
            legislation_refs_total += len(refs)
        elif norm_id_str:
            legislation_refs_total += 1
    legislation_ids = [n for n in legislation_ids if n and n != "—"]
    legislation_norms_total = len({n for n in legislation_ids if n})
    keywords = case_data.get("caseKeywords") or []

    links = []
    if case_content.get("caseUrl"):
        links.append({"label": "Pagina do processo", "url": case_content.get("caseUrl")})
    if identity.get("caseUrl"):
        links.append({"label": "URL de identificacao", "url": identity.get("caseUrl")})

    decision_final_raw = (
        (decision_details.get("decisionResult") or {}).get("finalDecision")
        if isinstance(decision_details, dict)
        else None
    )
    decision_final = _normalize_decision_label(decision_final_raw)

    minister_votes_raw = decision_details.get("ministerVotes") if isinstance(decision_details, dict) else []
    minister_votes = []
    if isinstance(minister_votes_raw, list):
        for vote in minister_votes_raw:
            if not isinstance(vote, dict):
                continue
            name = normalize_minister_name(vote.get("ministerName") or vote.get("minister"))
            vote_result = _normalize_vote_label(vote.get("voteType") or vote.get("vote"))
            if not name and not vote_result:
                continue
            minister_votes.append(
                {
                    "minister": name or "—",
                    "vote": vote_result or "Outros",
                }
            )

    return {
        "id": str(doc.get("_id")),
        "stf_id": identity.get("stfDecisionId") or str(doc.get("_id")),
        "title": identity.get("caseTitle") or doc.get("caseTitle") or "-",
        "case_class": identity.get("caseClass") or case_ident.get("caseClass") or "-",
        "rapporteur": identity.get("rapporteur") or case_ident.get("rapporteur") or "-",
        "judgment_date": _format_date(dates.get("judgmentDate")),
        "publication_date": _format_date(dates.get("publicationDate")),
        "judging_body": identity.get("judgingBody") or case_ident.get("judgingBody") or "-",
        "doctrine_refs": doctrine_refs,
        "keywords": keywords,
        "legislation_refs": legislation_refs,
        "legislation_ids": legislation_ids,
        "legislation_refs_total": legislation_refs_total,
        "legislation_norms_total": legislation_norms_total,
        "parties": parties,
        "parties_count": len(parties),
        "links": links,
        "decision_final": decision_final,
        "minister_votes": minister_votes,
    }


def _aggregate_process_kpis(collection: Collection, match: Dict[str, Any]) -> Dict[str, Any]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append(
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                },
                "_doctrineCount": {"$size": {"$ifNull": [f"${DOCTRINE_PATH}", []]}},
                "_legislationCount": {"$size": {"$ifNull": [f"${LEGISLATION_PATH}", []]}},
            }
        }
    )
    pipeline.append(
        {
            "$group": {
                "_id": None,
                "total_processes": {"$sum": 1},
                "rapporteurs": {"$addToSet": "$_rapporteur"},
                "total_doctrines": {"$sum": "$_doctrineCount"},
                "total_legislation": {"$sum": "$_legislationCount"},
            }
        }
    )
    result = list(collection.aggregate(pipeline))
    if not result:
        return {
            "total_processes": 0,
            "processes_per_rapporteur": None,
            "doctrines_per_case": None,
            "legislation_per_case": None,
        }

    row = result[0]
    total = int(row.get("total_processes") or 0)
    rapporteurs = [r for r in (row.get("rapporteurs") or []) if r]
    num_rapporteurs = len(rapporteurs)
    total_doctrines = int(row.get("total_doctrines") or 0)
    total_legislation = int(row.get("total_legislation") or 0)

    processes_per_rapporteur = None
    if total > 0 and num_rapporteurs > 0:
        processes_per_rapporteur = total / num_rapporteurs

    doctrines_per_case = None
    if total > 0:
        doctrines_per_case = round(total_doctrines / total)

    legislation_per_case = None
    if total > 0:
        legislation_per_case = round(total_legislation / total)

    return {
        "total_processes": total,
        "processes_per_rapporteur": processes_per_rapporteur,
        "doctrines_per_case": doctrines_per_case,
        "legislation_per_case": legislation_per_case,
    }


def _aggregate_author_insights(
    collection: Collection, author_name: str, match: Dict[str, Any]
) -> Dict[str, Any]:
    pipeline = [
        {"$match": match},
        {"$unwind": f"${DOCTRINE_PATH}"},
        {"$match": {f"{DOCTRINE_PATH}.author": _regex(author_name, exact=True)}},
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                },
                "_caseId": _case_id_expr(),
            }
        },
        {
            "$group": {
                "_id": None,
                "total_citations": {"$sum": 1},
                "unique_citations": {
                    "$addToSet": {
                        "case": "$_caseId",
                        "title": f"${DOCTRINE_PATH}.publicationTitle",
                    }
                },
                "top_work": {"$push": f"${DOCTRINE_PATH}.publicationTitle"},
                "rapporteurs": {"$push": "$_rapporteur"},
            }
        },
    ]
    result = list(collection.aggregate(pipeline))
    if not result:
        return {
            "total_citations": 0,
            "unique_citations": 0,
            "top_work": "—",
            "top_rapporteur": "—",
        }

    row = result[0]
    total_citations = int(row.get("total_citations") or 0)
    unique_citations = len(row.get("unique_citations") or [])

    top_work = "—"
    work_counts: Dict[str, int] = {}
    for item in row.get("top_work") or []:
        if not item:
            continue
        work_counts[item] = work_counts.get(item, 0) + 1
    if work_counts:
        top_work = sorted(work_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

    rapporteur_counts: Dict[str, int] = {}
    for item in row.get("rapporteurs") or []:
        if not item:
            continue
        rapporteur_counts[item] = rapporteur_counts.get(item, 0) + 1
    top_rapporteur = "—"
    if rapporteur_counts:
        top_rapporteur = sorted(rapporteur_counts.items(), key=lambda x: (-x[1], x[0]))[0][0]

    return {
        "total_citations": total_citations,
        "unique_citations": unique_citations,
        "top_work": top_work,
        "top_rapporteur": top_rapporteur,
    }


def _fetch_author_citations(
    collection: Collection,
    author_name: str,
    match: Dict[str, Any],
    limit: int = 50,
) -> List[Dict[str, Any]]:
    pipeline = [
        {"$match": match},
        {"$unwind": f"${DOCTRINE_PATH}"},
        {"$match": {f"{DOCTRINE_PATH}.author": _regex(author_name, exact=True)}},
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                },
                "_caseClass": {
                    "$ifNull": ["$identity.caseClass", "$caseIdentification.caseClass"]
                },
                "_caseTitle": {"$ifNull": ["$identity.caseTitle", "$caseTitle"]},
                "_caseId": _case_id_expr(),
                "_stfId": {"$ifNull": ["$identity.stfDecisionId", None]},
                "_caseUrl": {
                    "$ifNull": ["$caseContent.caseUrl", "$identity.caseUrl"]
                },
                "_judgmentDate": "$dates.judgmentDate",
            }
        },
        {
            "$project": {
                "_id": 0,
                "case_title": "$_caseTitle",
                "rapporteur": "$_rapporteur",
                "work": f"${DOCTRINE_PATH}.publicationTitle",
                "case_class": "$_caseClass",
                "judgment_date": "$_judgmentDate",
                "case_url": "$_caseUrl",
                "case_id": "$_caseId",
                "stf_id": "$_stfId",
            }
        },
        {"$sort": {"judgment_date": -1}},
    ]
    if limit:
        pipeline.append({"$limit": limit})
    rows = list(collection.aggregate(pipeline))
    for row in rows:
        row["judgment_date"] = _format_date(row.get("judgment_date"))
    return rows


def _aggregate_top_cases_by_doctrine(
    collection: Collection, match: Dict[str, Any], limit: int = 3
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append(
        {
            "$addFields": {
                "_doctrineCount": {"$size": {"$ifNull": [f"${DOCTRINE_PATH}", []]}},
                "_caseTitle": {"$ifNull": ["$identity.caseTitle", "$caseTitle"]},
                "_stfId": {"$ifNull": ["$identity.stfDecisionId", "$_id"]},
            }
        }
    )
    pipeline.append({"$match": {"_doctrineCount": {"$gt": 0}}})
    pipeline.append(
        {
            "$project": {
                "_id": 0,
                "case_title": "$_caseTitle",
                "stf_id": "$_stfId",
                "total": "$_doctrineCount",
            }
        }
    )
    pipeline.append({"$sort": {"total": -1, "case_title": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    return list(collection.aggregate(pipeline))


def _aggregate_minister_options(collection: Collection, case_match: Dict[str, Any]) -> List[str]:
    rapporteur_pipeline: List[Dict[str, Any]] = []
    if case_match:
        rapporteur_pipeline.append({"$match": case_match})
    rapporteur_pipeline.append(
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                }
            }
        }
    )
    rapporteur_pipeline.append({"$match": {"_rapporteur": {"$nin": [None, ""]}}})
    rapporteur_pipeline.append({"$group": {"_id": "$_rapporteur"}})
    rapporteur_pipeline.append({"$project": {"_id": 0, "label": "$_id"}})

    vote_pipeline: List[Dict[str, Any]] = []
    if case_match:
        vote_pipeline.append({"$match": case_match})
    vote_pipeline.append({"$unwind": f"${MINISTER_VOTES_PATH}"})
    vote_pipeline.append(
        {"$match": {f"{MINISTER_VOTES_PATH}.ministerName": {"$nin": [None, ""]}}}
    )
    vote_pipeline.append({"$group": {"_id": f"${MINISTER_VOTES_PATH}.ministerName"}})
    vote_pipeline.append({"$project": {"_id": 0, "label": "$_id"}})

    names = {row["label"] for row in collection.aggregate(rapporteur_pipeline)}
    names.update({row["label"] for row in collection.aggregate(vote_pipeline)})
    return sorted(names, key=str.casefold)


def _aggregate_case_classes(collection: Collection, case_match: Dict[str, Any]) -> List[str]:
    pipeline: List[Dict[str, Any]] = []
    if case_match:
        pipeline.append({"$match": case_match})
    pipeline.append(
        {
            "$addFields": {
                "_case_class": {
                    "$ifNull": ["$identity.caseClass", "$caseIdentification.caseClass"]
                }
            }
        }
    )
    pipeline.append({"$match": {"_case_class": {"$nin": [None, ""]}}})
    pipeline.append({"$group": {"_id": "$_case_class"}})
    pipeline.append({"$project": {"_id": 0, "label": "$_id"}})
    classes = [row["label"] for row in collection.aggregate(pipeline)]
    return sorted(classes, key=str.casefold)


def _aggregate_rapporteur_options(collection: Collection) -> List[str]:
    pipeline: List[Dict[str, Any]] = [
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                }
            }
        },
        {"$match": {"_rapporteur": {"$nin": [None, ""]}}},
        {"$group": {"_id": "$_rapporteur"}},
        {"$project": {"_id": 0, "label": "$_id"}},
    ]
    names = [row["label"] for row in collection.aggregate(pipeline)]
    return sorted(names, key=str.casefold)


def _aggregate_author_suggestions(collection: Collection, limit: int = 200) -> List[str]:
    pipeline: List[Dict[str, Any]] = [
        {"$unwind": f"${DOCTRINE_PATH}"},
        {"$match": {f"{DOCTRINE_PATH}.author": {"$nin": [None, ""]}}},
        {"$group": {"_id": f"${DOCTRINE_PATH}.author"}},
        {"$project": {"_id": 0, "label": "$_id"}},
        {"$sort": {"label": 1}},
    ]
    if limit:
        pipeline.append({"$limit": limit})
    return [row["label"] for row in collection.aggregate(pipeline)]


def _aggregate_ministers(collection: Collection, filters: Dict[str, str]) -> List[Dict[str, Any]]:
    case_match = _build_ministro_case_match(filters)
    minister_value = filters.get("minister") or ""
    if minister_value:
        minister_value = normalize_minister_name(minister_value) or minister_value
    minister_regex = _regex(minister_value, exact=True) if minister_value else None
    all_citation_types = [
        "doutrina",
        "legislacao",
        "precedente_vinculante",
        "precedente_persuasivo",
        "jurisprudencia",
        "outro",
    ]

    rapporteur_pipeline: List[Dict[str, Any]] = []
    if case_match:
        rapporteur_pipeline.append({"$match": case_match})
    rapporteur_pipeline.append({"$addFields": {"_rapporteur": "$identity.rapporteur"}})
    if minister_regex:
        rapporteur_pipeline.append({"$match": {"_rapporteur": minister_regex}})
    rapporteur_pipeline.append({"$match": {"_rapporteur": {"$nin": [None, ""]}}})
    rapporteur_pipeline.append(
        {
            "$group": {
                "_id": "$_rapporteur",
                "case_ids": {"$addToSet": _case_id_expr()},
            }
        }
    )
    rapporteur_pipeline.append(
        {
            "$project": {
                "_id": 0,
                "label": "$_id",
                "case_ids": 1,
                "total_relatorias": {"$size": "$case_ids"},
            }
        }
    )

    vote_pipeline: List[Dict[str, Any]] = []
    if case_match:
        vote_pipeline.append({"$match": case_match})
    vote_pipeline.append({"$unwind": f"${MINISTER_VOTES_PATH}"})
    vote_pipeline.append(
        {
            "$addFields": {
                "_minister": f"${MINISTER_VOTES_PATH}.ministerName",
                "_voteType": {"$ifNull": [f"${MINISTER_VOTES_PATH}.voteType", ""]},
            }
        }
    )
    if minister_regex:
        vote_pipeline.append({"$match": {"_minister": minister_regex}})
    vote_pipeline.append({"$match": {"_minister": {"$nin": [None, ""]}}})
    vote_pipeline.append(
        {
            "$group": {
                "_id": "$_minister",
                "case_ids": {"$addToSet": _case_id_expr()},
                "total_votes_defined": {
                    "$sum": {
                        "$cond": [
                            {"$ne": ["$_voteType", ""]},
                            1,
                            0,
                        ]
                    }
                },
                "total_votes_pending": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$_voteType", ""]},
                            1,
                            0,
                        ]
                    }
                },
                "total_votes_vencido": {
                    "$sum": {
                        "$cond": [
                            {"$eq": ["$_voteType", "vencido"]},
                            1,
                            0,
                        ]
                    }
                },
            }
        }
    )
    vote_pipeline.append(
        {
            "$project": {
                "_id": 0,
                "label": "$_id",
                "case_ids": 1,
                "total_votes_defined": 1,
                "total_votes_pending": 1,
                "total_votes_vencido": 1,
            }
        }
    )

    citations_pipeline: List[Dict[str, Any]] = []
    if case_match:
        citations_pipeline.append({"$match": case_match})
    citations_pipeline.append(
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                },
                "_voteMinisters": {
                    "$map": {
                        "input": {"$ifNull": [f"${MINISTER_VOTES_PATH}", []]},
                        "as": "vote",
                        "in": "$$vote.ministerName",
                    }
                },
                "_citationsCount": _citations_count_expr(all_citation_types),
            }
        }
    )
    citations_pipeline.append(
        {"$addFields": {"_ministers": {"$setUnion": [["$_rapporteur"], "$_voteMinisters"]}}}
    )
    citations_pipeline.append({"$unwind": "$_ministers"})
    citations_pipeline.append({"$match": {"_ministers": {"$nin": [None, ""]}}})
    if minister_regex:
        citations_pipeline.append({"$match": {"_ministers": minister_regex}})
    citations_pipeline.append(
        {
            "$group": {
                "_id": "$_ministers",
                "citations_total": {"$sum": "$_citationsCount"},
            }
        }
    )
    citations_pipeline.append(
        {"$project": {"_id": 0, "label": "$_id", "citations_total": 1}}
    )

    stats: Dict[str, Dict[str, Any]] = {}
    for row in collection.aggregate(rapporteur_pipeline):
        raw_name = row.get("label") or ""
        name = normalize_minister_name(raw_name) or raw_name
        entry = stats.get(name)
        if not entry:
            entry = {
                "minister": name,
                "case_ids": set(),
                "relatoria_case_ids": set(),
                "total_relatorias": 0,
                "citations_total": 0,
                "total_votes_defined": 0,
                "total_votes_pending": 0,
                "total_votes_vencido": 0,
            }
            stats[name] = entry
        entry["relatoria_case_ids"].update(row.get("case_ids") or [])
        entry["case_ids"].update(row.get("case_ids") or [])
        entry["total_relatorias"] = len(entry["relatoria_case_ids"])

    for row in collection.aggregate(vote_pipeline):
        raw_name = row.get("label") or ""
        name = normalize_minister_name(raw_name) or raw_name
        entry = stats.get(name)
        if not entry:
            entry = {
                "minister": name,
                "case_ids": set(),
                "relatoria_case_ids": set(),
                "total_relatorias": 0,
                "citations_total": 0,
                "total_votes_defined": 0,
                "total_votes_pending": 0,
                "total_votes_vencido": 0,
            }
            stats[name] = entry
        entry["case_ids"].update(row.get("case_ids") or [])
        entry["total_votes_defined"] += row.get("total_votes_defined") or 0
        entry["total_votes_pending"] += row.get("total_votes_pending") or 0
        entry["total_votes_vencido"] += row.get("total_votes_vencido") or 0

    for row in collection.aggregate(citations_pipeline):
        raw_name = row.get("label") or ""
        name = normalize_minister_name(raw_name) or raw_name
        entry = stats.get(name)
        if not entry:
            entry = {
                "minister": name,
                "case_ids": set(),
                "relatoria_case_ids": set(),
                "total_relatorias": 0,
                "citations_total": 0,
                "total_votes_defined": 0,
                "total_votes_pending": 0,
                "total_votes_vencido": 0,
            }
            stats[name] = entry
        entry["citations_total"] = row.get("citations_total") or 0

    results: List[Dict[str, Any]] = []
    for entry in stats.values():
        results.append(
            {
                "minister": entry["minister"],
                "total_processes": len(entry["case_ids"]),
                "total_relatorias": entry["total_relatorias"],
                "citations_total": entry["citations_total"],
                "total_votes_vencido": entry["total_votes_vencido"],
                "total_votes_defined": entry["total_votes_defined"],
                "total_votes_pending": entry["total_votes_pending"],
            }
        )

    results.sort(key=lambda item: (-item["total_processes"], item["minister"].casefold()))
    return results


def _build_minister_match(filters: Dict[str, str], minister_name: str) -> Dict[str, Any]:
    case_match = _build_ministro_case_match(filters)
    minister_name = normalize_minister_name(minister_name) or minister_name
    minister_regex = _regex(minister_name, exact=True)
    minister_match = {
        "$or": [
            {"identity.rapporteur": minister_regex},
            {"caseIdentification.rapporteur": minister_regex},
            {MINISTER_VOTES_PATH: {"$elemMatch": {"ministerName": minister_regex}}},
        ]
    }
    if not case_match:
        return minister_match
    return {"$and": [case_match, minister_match]}


def _build_relatoria_match(filters: Dict[str, str], minister_name: str) -> Dict[str, Any]:
    case_match = _build_ministro_case_match(filters)
    minister_name = normalize_minister_name(minister_name) or minister_name
    minister_regex = _regex(minister_name, exact=True)
    relatoria_match = {
        "$or": [
            {"identity.rapporteur": minister_regex},
            {"caseIdentification.rapporteur": minister_regex},
        ]
    }
    if not case_match:
        return relatoria_match
    return {"$and": [case_match, relatoria_match]}


def _count_distinct_cases(collection: Collection, match: Dict[str, Any]) -> int:
    pipeline: List[Dict[str, Any]] = [
        {"$match": match},
        {"$group": {"_id": _case_id_expr()}},
        {"$count": "total"},
    ]
    result = list(collection.aggregate(pipeline))
    return int(result[0]["total"]) if result else 0


def _aggregate_minister_detail(
    collection: Collection, filters: Dict[str, str], minister_name: str
) -> Dict[str, Any]:
    match = _build_minister_match(filters, minister_name)
    all_citation_types = [
        "doutrina",
        "legislacao",
        "precedente_vinculante",
        "precedente_persuasivo",
        "jurisprudencia",
        "outro",
    ]
    norm_citation_types = [
        "legislacao",
        "precedente_vinculante",
        "precedente_persuasivo",
        "jurisprudencia",
    ]

    total_processes = _count_distinct_cases(collection, match)

    relatoria_match = _build_relatoria_match(filters, minister_name)
    total_relatorias = _count_distinct_cases(collection, relatoria_match)

    citation_stats = _compute_minister_reference_stats(collection, match)
    total_citations = citation_stats["total_citations"]
    decision_distribution = citation_stats["decision_distribution"]
    top_doctrine = citation_stats["top_doctrine"]
    top_norms = citation_stats["top_acordaos"]
    top_legislation = citation_stats["top_legislation"]

    return {
        "total_processes": total_processes,
        "total_relatorias": total_relatorias,
        "total_citations": total_citations,
        "decision_distribution": decision_distribution,
        "top_doctrine": top_doctrine,
        "top_norms": top_norms,
        "top_legislation": top_legislation,
    }


def _aggregate_top_legislation_for_minister(
    collection: Collection, match: Dict[str, Any], limit: int = 5
) -> List[Dict[str, Any]]:
    pipeline: List[Dict[str, Any]] = []
    if match:
        pipeline.append({"$match": match})
    pipeline.append({"$unwind": f"${LEGISLATION_PATH}"})
    pipeline.append({"$addFields": {"_normId": f"${LEGISLATION_PATH}.normIdentifier"}})
    pipeline.append({"$match": {"_normId": {"$nin": [None, ""]}}})
    pipeline.append(
        {
            "$group": {
                "_id": "$_normId",
                "total": {"$sum": 1},
            }
        }
    )
    pipeline.append(
        {
            "$project": {
                "_id": 0,
                "identifier": "$_id",
                "total": 1,
            }
        }
    )
    pipeline.append({"$sort": {"total": -1, "identifier": 1}})
    if limit:
        pipeline.append({"$limit": limit})
    return list(collection.aggregate(pipeline))


def _normalize_ref_text(value: Any) -> str:
    if value is None:
        return ""
    return " ".join(str(value).split()).strip()


def _compute_minister_reference_stats(
    collection: Collection, match: Dict[str, Any]
) -> Dict[str, Any]:
    from collections import Counter

    projection = {
        "identity.stfDecisionId": 1,
        "caseData.legislationReferences": 1,
        "caseData.doctrineReferences": 1,
        "caseData.notesReferences": 1,
        DECISION_RESULT_PATH: 1,
    }
    cursor = collection.find(match, projection=projection)

    total_citations = 0
    legislation_counter: Counter[str] = Counter()
    doctrine_counter: Counter[str] = Counter()
    acordao_counter: Counter[str] = Counter()
    decision_counter: Counter[str] = Counter()

    for doc in cursor:
        case_id = (doc.get("identity") or {}).get("stfDecisionId") or str(doc.get("_id"))
        seen = set()

        case_data = doc.get("caseData") or {}
        legislation_refs = case_data.get("legislationReferences") or []
        doctrine_refs = case_data.get("doctrineReferences") or []
        notes_refs = case_data.get("notesReferences") or []

        for norm in legislation_refs:
            if not isinstance(norm, dict):
                ref_id = ""
            else:
                ref_id = _normalize_ref_text(norm.get("normIdentifier"))
            if not ref_id:
                continue
            key = (case_id, "legislacao", ref_id)
            if key in seen:
                continue
            seen.add(key)
            total_citations += 1
            legislation_counter[ref_id] += 1

        for ref in doctrine_refs:
            if not isinstance(ref, dict):
                ref_id = _normalize_ref_text(ref)
            else:
                ref_id = _normalize_ref_text(
                    ref.get("rawCitation")
                    or ref.get("publicationTitle")
                    or ref.get("author")
                )
            if not ref_id:
                continue
            key = (case_id, "doutrina", ref_id)
            if key in seen:
                continue
            seen.add(key)
            total_citations += 1
            doctrine_counter[ref_id] += 1

        for note in notes_refs:
            if not isinstance(note, dict):
                continue
            note_type = str(note.get("noteType") or "notes").strip() or "notes"
            items = note.get("items") if isinstance(note.get("items"), list) else []
            if items:
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    raw_ref = _normalize_ref_text(item.get("rawRef"))
                    if not raw_ref:
                        continue
                    key = (case_id, f"notes:{note_type}", raw_ref)
                    if key in seen:
                        continue
                    seen.add(key)
                    total_citations += 1
                    if note_type == "stf_acordao":
                        acordao_counter[raw_ref] += 1
            else:
                raw_ref = _normalize_ref_text(note.get("rawLine"))
                if not raw_ref:
                    continue
                key = (case_id, f"notes:{note_type}", raw_ref)
                if key in seen:
                    continue
                seen.add(key)
                total_citations += 1
                if note_type == "stf_acordao":
                    acordao_counter[raw_ref] += 1

        decision_raw = _normalize_ref_text(
            (doc.get("caseData") or {}).get("decisionDetails", {}).get("decisionResult", {}).get("finalDecision")
        )
        decision_label = decision_raw or "Não informado"
        decision_counter[decision_label] += 1

    def _top_items(counter: Counter[str], limit: int = 5) -> List[Dict[str, Any]]:
        items = sorted(counter.items(), key=lambda x: (-x[1], x[0]))
        return [{"label": k, "total": v} for k, v in items[:limit]]

    decision_distribution = [
        {"label": k, "total": v}
        for k, v in sorted(decision_counter.items(), key=lambda x: (-x[1], x[0]))
    ]

    return {
        "total_citations": total_citations,
        "top_legislation": _top_items(legislation_counter, limit=5),
        "top_doctrine": _top_items(doctrine_counter, limit=5),
        "top_acordaos": _top_items(acordao_counter, limit=5),
        "decision_distribution": decision_distribution,
    }


def _fetch_relatoria_cases(collection: Collection, match: Dict[str, Any]) -> List[Dict[str, Any]]:
    projection = {
        "identity.stfDecisionId": 1,
        "identity.caseTitle": 1,
        "identity.caseClass": 1,
        "identity.caseNumber": 1,
        "caseIdentification.caseClass": 1,
        "caseIdentification.caseNumber": 1,
        "caseTitle": 1,
        "dates.judgmentDate": 1,
        DECISION_RESULT_PATH: 1,
    }
    cursor = collection.find(match, projection=projection).sort("dates.judgmentDate", -1)
    rows: List[Dict[str, Any]] = []
    for doc in cursor:
        identity = doc.get("identity") or {}
        case_ident = doc.get("caseIdentification") or {}
        decision_details = (doc.get("caseData") or {}).get("decisionDetails") or {}

        case_title = identity.get("caseTitle") or doc.get("caseTitle") or "-"
        stf_id = identity.get("stfDecisionId") or str(doc.get("_id"))
        case_class = identity.get("caseClass") or case_ident.get("caseClass") or ""
        case_number = identity.get("caseNumber") or case_ident.get("caseNumber") or ""
        if case_class and case_number:
            process_label = f"{case_class} {case_number}"
        else:
            process_label = case_class or case_number or "-"

        decision_final = (
            (decision_details.get("decisionResult") or {}).get("finalDecision") or "—"
        )

        rows.append(
            {
                "case_title": case_title,
                "stf_id": stf_id,
                "process_label": process_label,
                "decision_final": decision_final,
            }
        )
    return rows


def _build_query_detail(case_query_doc: Dict[str, Any]) -> Dict[str, Any]:
    url_params = _parse_query_url(str(case_query_doc.get("queryUrl") or ""))
    query_string = case_query_doc.get("queryString") or url_params.get("query_string") or "—"
    page_size = case_query_doc.get("pageSize") or url_params.get("page_size") or "—"
    full_text = case_query_doc.get("inteiroTeor")
    full_text = "true" if full_text is True else "false" if full_text is False else (url_params.get("full_text") or "—")
    process_class_sigla = url_params.get("process_class_sigla") or []
    if not process_class_sigla:
        process_class_sigla = ["—"]

    return {
        "termo": query_string,
        "classe": process_class_sigla,
        "data_inicial": "—",
        "data_final": "—",
        "resultados_por_pagina": page_size,
        "pagina": url_params.get("page") or "—",
        "ordenacao": url_params.get("sort") or "—",
        "ordem": url_params.get("sort_by") or "—",
        "inteiro_teor": full_text,
        "url": case_query_doc.get("queryUrl") or "—",
    }


def _compute_step_summary(
    case_data_col: Collection,
    base_match: Dict[str, Any],
    *,
    status_field: str,
    success_values: List[str],
    error_values: List[str],
    start_field: str,
    end_field: str,
) -> Dict[str, Any]:
    pipeline = [
        {"$match": base_match},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "success": {
                    "$sum": {"$cond": [{"$in": [f"${status_field}", success_values]}, 1, 0]}
                },
                "failed": {
                    "$sum": {"$cond": [{"$in": [f"${status_field}", error_values]}, 1, 0]}
                },
                "startedAt": {"$min": f"${start_field}"},
                "finishedAt": {"$max": f"${end_field}"},
            }
        },
    ]
    result = list(case_data_col.aggregate(pipeline))
    if not result:
        return {"status": "scheduled", "total": 0, "started_at": None, "finished_at": None}

    row = result[0]
    total = int(row.get("total") or 0)
    success = int(row.get("success") or 0)
    failed = int(row.get("failed") or 0)

    started_at = row.get("startedAt")
    if total == 0:
        status = "scheduled"
    elif failed > 0:
        status = "failed"
    elif success >= total:
        status = "completed"
    elif started_at is None:
        status = "scheduled"
    else:
        status = "running"

    return {
        "status": status,
        "total": total,
        "success": success,
        "error": failed,
        "remaining": max(total - success - failed, 0),
        "started_at": started_at,
        "finished_at": row.get("finishedAt"),
    }


def _compute_processing_step_summary(
    case_data_col: Collection,
    base_match: Dict[str, Any],
    *,
    success_field: str,
    error_field: str,
    start_field: str,
    end_field: str,
) -> Dict[str, Any]:
    pipeline = [
        {"$match": base_match},
        {
            "$group": {
                "_id": None,
                "total": {"$sum": 1},
                "success": {
                    "$sum": {"$cond": [{"$ne": [f"${success_field}", None]}, 1, 0]}
                },
                "failed": {
                    "$sum": {"$cond": [{"$ne": [f"${error_field}", None]}, 1, 0]}
                },
                "startedAt": {"$min": f"${start_field}"},
                "finishedAt": {"$max": f"${end_field}"},
            }
        },
    ]
    result = list(case_data_col.aggregate(pipeline))
    if not result:
        return {"status": "scheduled", "total": 0, "started_at": None, "finished_at": None}

    row = result[0]
    total = int(row.get("total") or 0)
    success = int(row.get("success") or 0)
    failed = int(row.get("failed") or 0)

    started_at = row.get("startedAt")
    if total == 0:
        status = "scheduled"
    elif failed > 0:
        status = "failed"
    elif success >= total:
        status = "completed"
    elif started_at is None:
        status = "scheduled"
    else:
        status = "running"

    return {
        "status": status,
        "total": total,
        "success": success,
        "error": failed,
        "remaining": max(total - success - failed, 0),
        "started_at": started_at,
        "finished_at": row.get("finishedAt"),
    }


def _step_summary_for_script(
    case_query_doc: Dict[str, Any],
    case_data_col: Collection,
    base_match: Dict[str, Any],
    script: str,
) -> Dict[str, Any]:
    script = script.strip()
    if script == "step01-extract-cases.py":
        status = str(case_query_doc.get("status") or "unknown")
        total = int(case_query_doc.get("extractedCount") or 0)
        return {
            "status": status if status in {"new", "extracting", "extracted", "error"} else "unknown",
            "total": total,
            "success": total if status == "extracted" else 0,
            "error": total if status == "error" else 0,
            "remaining": 0 if status in {"extracted", "error"} else total,
            "started_at": case_query_doc.get("extractingAt"),
            "finished_at": case_query_doc.get("processedDate"),
        }
    if script == "step02-get-case-html.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseScrapeStatus",
            success_values=["success"],
            error_values=["error", "challenge"],
            start_field="processing.caseScrapeAt",
            end_field="processing.caseScrapeAt",
        )
    if script == "step03-clean-case-html.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseHtmlCleanStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseHtmlCleaningAt",
            end_field="processing.caseHtmlCleanedAt",
        )
    if script == "step04-extract-sessions.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseSectionsStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseSectionsExtractingAt",
            end_field="processing.caseSectionsExtractedAt",
        )
    if script == "step05-extract-keywords-parties.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.partiesKeywordsStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.partiesKeywords.finishedAt",
            end_field="processing.partiesKeywords.finishedAt",
        )
    if script == "step06-extract-legislation-mistral.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseLegislationRefsStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseLegislationRefsAt",
            end_field="processing.caseLegislationRefsAt",
        )
    if script == "step07-extract-notes-mistral.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseNotesRefsStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseNotesRefsAt",
            end_field="processing.caseNotesRefsAt",
        )
    if script == "step08-doctrine-mistral.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseDoctrineStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseDoctrineAt",
            end_field="processing.caseDoctrineAt",
        )
    if script == "step09-extract-decision-details-mistral.py":
        return _compute_step_summary(
            case_data_col,
            base_match,
            status_field="processing.caseDecisionDetailsStatus",
            success_values=["success"],
            error_values=["error"],
            start_field="processing.caseDecisionDetailsAt",
            end_field="processing.caseDecisionDetailsAt",
        )
    return {
        "status": "unknown",
        "total": 0,
        "success": 0,
        "error": 0,
        "remaining": 0,
        "started_at": None,
        "finished_at": None,
    }

app = Flask(__name__)


@app.route("/")
def index() -> Any:
    return redirect(url_for("doutrina"))


@app.route("/doutrina")
def doutrina() -> Any:
    filters = _get_filters(request.args)
    collection = _get_collection()
    author_limit = _limit_value(request.args.get("author_limit"), default=10)
    title_limit = _limit_value(request.args.get("title_limit"), default=10)

    base_match = _build_match(filters)
    citation_types_all = [
        "doutrina",
        "legislacao",
        "precedente_vinculante",
        "precedente_persuasivo",
        "jurisprudencia",
        "outro",
    ]

    summary_total = _count_cases(collection, filters)
    authors = _aggregate_authors(collection, filters)
    titles = _aggregate_titles(collection, filters)
    rapporteurs = _aggregate_rapporteurs(collection, filters)
    top_doctrine_titles = _aggregate_top_doctrine_titles(collection, base_match, limit=3)
    top_cases_by_doctrine = _aggregate_top_cases_by_doctrine(collection, base_match, limit=3)
    avg_citations = _aggregate_avg_citations(collection, base_match, citation_types_all)
    vote_vencido_rate = _aggregate_vote_vencido_rate(collection, base_match)
    decision_distribution = _aggregate_decision_distribution(collection, base_match)
    citation_ratio = _aggregate_citation_ratio(collection, base_match)
    cases_by_year = _aggregate_cases_by_year(collection, base_match)
    cases_per_year_avg = _calculate_cases_per_year_avg(cases_by_year)
    case_trend = _calculate_case_trend(cases_by_year)

    total_decisions = sum(item.get("total", 0) for item in decision_distribution)
    decision_distribution_pct = []
    if total_decisions:
        for item in decision_distribution:
            decision_distribution_pct.append(
                {
                    "label": item.get("label", "Nao informado"),
                    "total": item.get("total", 0),
                    "percent": (item.get("total", 0) / total_decisions) * 100,
                }
            )

    case_trend_label = "—"
    if case_trend:
        change = case_trend.get("change")
        year_label = case_trend.get("year")
        if change is not None and year_label is not None:
            sign = "+" if change >= 0 else ""
            case_trend_label = f"{sign}{change:.1f}% em {year_label}"

    filter_params = {k: v for k, v in filters.items() if v}
    next_author_limit = author_limit + 50
    next_title_limit = title_limit + 50

    return render_template(
        "doutrina.html",
        title="CITO | Doutrina",
        filters=filters,
        filter_params=filter_params,
        summary_total=summary_total,
        authors=authors[:author_limit],
        titles=titles[:title_limit],
        authors_total=len(authors),
        titles_total=len(titles),
        author_limit=author_limit,
        title_limit=title_limit,
        next_author_limit=next_author_limit,
        next_title_limit=next_title_limit,
        rapporteurs=rapporteurs,
        top_doctrine_titles=top_doctrine_titles,
        top_cases_by_doctrine=top_cases_by_doctrine,
        avg_citations=avg_citations,
        vote_vencido_rate=vote_vencido_rate,
        decision_distribution_pct=decision_distribution_pct,
        citation_ratio=citation_ratio,
        cases_per_year_avg=cases_per_year_avg,
        case_trend_label=case_trend_label,
        chart_minister_labels=[row["label"] for row in rapporteurs[:10]],
        chart_minister_values=[row["total"] for row in rapporteurs[:10]],
        chart_decision_labels=[row["label"] for row in decision_distribution_pct],
        chart_decision_values=[row["percent"] for row in decision_distribution_pct],
        chart_year_labels=[row["year"] for row in cases_by_year],
        chart_year_values=[row["total"] for row in cases_by_year],
    )


@app.route("/doutrina/detalhe")
def doutrina_detail() -> Any:
    filters = _get_filters(request.args)
    kind = str(request.args.get("kind") or "").strip().lower()
    value = str(request.args.get("value") or "").strip()
    if not value:
        return redirect(url_for("doutrina", **{k: v for k, v in filters.items() if v}))

    overrides: Dict[str, Tuple[str, bool]] = {}
    label_map = {
        "author": "Author",
        "title": "Publication Title",
        "rapporteur": "Rapporteur",
    }

    if kind == "author":
        overrides["author"] = (value, True)
    elif kind == "title":
        overrides["title"] = (value, True)
    else:
        kind = "rapporteur"
        overrides["rapporteur"] = (value, True)

    collection = _get_collection()
    limit = _limit_value(request.args.get("limit"), default=10)

    match = _build_match(filters, overrides=overrides)
    total_cases = collection.count_documents(match)
    cases = _fetch_cases(collection, match, limit=limit)

    author_insights = None
    citations = None
    author_show_more = False
    if kind == "author":
        author_insights = _aggregate_author_insights(collection, value, match)
        citations = _fetch_author_citations(collection, value, match, limit=limit)
        author_show_more = (author_insights.get("total_citations", 0) > limit) if author_insights else False

    filter_params = {k: v for k, v in filters.items() if v}
    next_limit = limit + 50

    return render_template(
        "doutrina_detail.html",
        title="CITO | Doutrina | Detail",
        detail_kind=label_map.get(kind, kind).upper(),
        detail_key=kind,
        detail_value=value,
        total=total_cases,
        cases=cases,
        author_insights=author_insights,
        citations=citations,
        author_show_more=author_show_more,
        filter_params=filter_params,
        limit=limit,
        next_limit=next_limit,
        show_more=total_cases > limit,
    )


@app.route("/ministros")
def ministros() -> Any:
    filters = _get_ministro_filters(request.args)
    collection = _get_collection()
    case_match = _build_ministro_case_match(filters)
    citation_types_all = [
        "doutrina",
        "legislacao",
        "precedente_vinculante",
        "precedente_persuasivo",
        "jurisprudencia",
        "outro",
    ]

    ministers_list = _aggregate_ministers(collection, filters)
    total_ministers = len(ministers_list)
    limit = _limit_value(request.args.get("limit"), default=10)
    visible_ministers = ministers_list[:limit]
    next_limit = limit + 50

    ministers_totals = {
        "total_processes": sum(item.get("total_processes", 0) for item in ministers_list),
        "total_relatorias": sum(item.get("total_relatorias", 0) for item in ministers_list),
        "citations_total": sum(item.get("citations_total", 0) for item in ministers_list),
        "total_votes_vencido": sum(item.get("total_votes_vencido", 0) for item in ministers_list),
        "total_votes_defined": sum(item.get("total_votes_defined", 0) for item in ministers_list),
        "total_votes_pending": sum(item.get("total_votes_pending", 0) for item in ministers_list),
    }

    minister_options = _aggregate_minister_options(collection, case_match)
    class_options = _aggregate_case_classes(collection, case_match)

    total_cases = _count_distinct_cases(collection, case_match or {})
    avg_citations = _aggregate_avg_citations(collection, case_match, citation_types_all)
    vote_vencido_rate = _aggregate_vote_vencido_rate(collection, case_match)
    decision_distribution = _aggregate_decision_distribution(collection, case_match)
    citation_ratio = _aggregate_citation_ratio(collection, case_match)
    cases_by_year = _aggregate_cases_by_year(collection, case_match)
    cases_per_year_avg = _calculate_cases_per_year_avg(cases_by_year)
    case_trend = _calculate_case_trend(cases_by_year)

    total_decisions = sum(item.get("total", 0) for item in decision_distribution)
    decision_distribution_pct = []
    if total_decisions:
        for item in decision_distribution:
            decision_distribution_pct.append(
                {
                    "label": item.get("label", "Nao informado"),
                    "total": item.get("total", 0),
                    "percent": (item.get("total", 0) / total_decisions) * 100,
                }
            )

    top_relatoria = sorted(
        ministers_list,
        key=lambda item: (-item.get("total_relatorias", 0), item["minister"].casefold()),
    )
    top_relatoria_minister = top_relatoria[0] if top_relatoria else None

    case_trend_label = "—"
    if case_trend:
        change = case_trend.get("change")
        year_label = case_trend.get("year")
        if change is not None and year_label is not None:
            sign = "+" if change >= 0 else ""
            case_trend_label = f"{sign}{change:.1f}% em {year_label}"

    filter_params = {k: v for k, v in filters.items() if v}
    filter_params_no_minister = {k: v for k, v in filter_params.items() if k != "minister"}

    return render_template(
        "ministros.html",
        title="CITO | Ministros",
        filters=filters,
        filter_params=filter_params,
        filter_params_no_minister=filter_params_no_minister,
        minister_options=minister_options,
        class_options=class_options,
        total_ministers=total_ministers,
        ministers=visible_ministers,
        ministers_totals=ministers_totals,
        limit=limit,
        next_limit=next_limit,
        show_more=total_ministers > limit,
        total_cases=total_cases,
        avg_citations=avg_citations,
        vote_vencido_rate=vote_vencido_rate,
        decision_distribution_pct=decision_distribution_pct,
        citation_ratio=citation_ratio,
        cases_per_year_avg=cases_per_year_avg,
        case_trend_label=case_trend_label,
        top_relatoria_minister=top_relatoria_minister,
        chart_minister_labels=[row["minister"] for row in ministers_list[:10]],
        chart_minister_values=[row["total_processes"] for row in ministers_list[:10]],
        chart_decision_labels=[row["label"] for row in decision_distribution_pct],
        chart_decision_values=[row["percent"] for row in decision_distribution_pct],
        chart_year_labels=[row["year"] for row in cases_by_year],
        chart_year_values=[row["total"] for row in cases_by_year],
    )


@app.route("/ministros/detalhe")
def ministro_detail() -> Any:
    filters = _get_ministro_filters(request.args)
    minister_name = str(request.args.get("minister") or "").strip()
    if not minister_name:
        return redirect(url_for("ministros", **{k: v for k, v in filters.items() if v}))

    collection = _get_collection()
    details = _aggregate_minister_detail(collection, filters, minister_name)
    relatoria_match = _build_relatoria_match(filters, minister_name)
    relatoria_cases = _fetch_relatoria_cases(collection, relatoria_match)

    filter_params = {k: v for k, v in filters.items() if v}
    filter_params_no_minister = {k: v for k, v in filter_params.items() if k != "minister"}

    return render_template(
        "ministro_detail.html",
        title="CITO | Ministros | Detalhe",
        minister_name=minister_name,
        labels=MINISTER_DETAIL_LABELS,
        filter_params=filter_params,
        filter_params_no_minister=filter_params_no_minister,
        total_processes=details["total_processes"],
        total_relatorias=details["total_relatorias"],
        total_citations=details["total_citations"],
        decision_distribution=details["decision_distribution"],
        top_doctrine=details["top_doctrine"],
        top_norms=details["top_norms"],
        top_legislation=details["top_legislation"],
        relatoria_cases=relatoria_cases,
    )


@app.route("/scraping")
def scraping() -> Any:
    defaults = _load_query_defaults()
    status_meta = _status_meta()

    jobs_col = _get_scrape_jobs_collection()
    runs_col = _get_case_query_collection()

    scheduled_jobs = list(jobs_col.find({}).sort("scheduledFor", 1))
    recent_runs = list(runs_col.find({}).sort("extractionTimestamp", -1).limit(30))

    jobs_view = []
    for job in scheduled_jobs:
        status = str(job.get("status") or "scheduled")
        meta = status_meta.get(status, status_meta["unknown"])
        query = job.get("query") if isinstance(job.get("query"), dict) else {}
        classes = query.get("processClassSigla") or []
        jobs_view.append(
            {
                "id": str(job.get("_id")),
                "status": status,
                "status_label": meta["label"],
                "status_class": meta["class"],
                "scheduled_for": _format_datetime(job.get("scheduledFor")),
                "created_at": _format_datetime(job.get("createdAt")),
                "query_string": query.get("queryString") or "—",
                "page_size": query.get("pageSize") or "—",
                "classes": classes,
                "result_count": job.get("resultCount"),
            }
        )

    runs_view = []
    for run in recent_runs:
        status = str(run.get("status") or "unknown")
        meta = status_meta.get(status, status_meta["unknown"])
        url_params = _parse_query_url(str(run.get("queryUrl") or ""))
        classes = url_params.get("process_class_sigla") or []
        runs_view.append(
            {
                "id": str(run.get("_id")),
                "status": status,
                "status_label": meta["label"],
                "status_class": meta["class"],
                "started_at": _format_datetime(run.get("extractionTimestamp")),
                "finished_at": _format_datetime(run.get("processedDate")),
                "query_string": run.get("queryString") or url_params.get("query_string") or "—",
                "page_size": run.get("pageSize") or url_params.get("page_size") or "—",
                "classes": classes,
                "result_count": run.get("extractedCount") or 0,
            }
        )

    alerts = []
    for run in recent_runs:
        if str(run.get("status")) == "error":
            alerts.append(
                {
                    "title": "Falha na execucao do scraping",
                    "detail": f"Query '{run.get('queryString') or 'Sem termo'}' falhou.",
                    "time": _format_datetime(run.get("processedDate") or run.get("extractionTimestamp")),
                }
            )
    for job in scheduled_jobs:
        if str(job.get("status")) == "failed":
            alerts.append(
                {
                    "title": "Execucao agendada falhou",
                    "detail": job.get("error") or "Falha nao especificada.",
                    "time": _format_datetime(job.get("updatedAt") or job.get("scheduledFor")),
                }
            )

    return render_template(
        "scraping.html",
        title="CITO | Scraping",
        brand_sub="Scraping",
        hero_copy="Gerencie o agendamento e acompanhe o historico das execucoes do scraper do STF.",
        defaults=defaults,
        scheduled_jobs=jobs_view,
        recent_runs=runs_view,
        alerts=alerts,
    )


@app.route("/scraping/schedule", methods=["POST"])
def scraping_schedule() -> Any:
    defaults = _load_query_defaults()
    execution_mode = (request.form.get("execution_mode") or "schedule").strip().lower()
    scheduled_for = _parse_datetime_local(request.form.get("scheduled_for") or "")
    if execution_mode == "now" or scheduled_for is None:
        scheduled_for = datetime.now()

    query = {
        "queryString": (request.form.get("query_string") or defaults.get("query_string") or "").strip(),
        "fullText": _as_bool(request.form.get("full_text"), defaults.get("full_text", True)),
        "page": int(request.form.get("page") or defaults.get("page") or 1),
        "pageSize": int(request.form.get("page_size") or defaults.get("page_size") or 50),
        "sort": (request.form.get("sort") or defaults.get("sort") or "_score").strip(),
        "sortBy": (request.form.get("sort_by") or defaults.get("sort_by") or "desc").strip(),
        "requestDelaySeconds": float(request.form.get("request_delay_seconds") or defaults.get("request_delay_seconds") or 0),
        "sslVerify": _as_bool(request.form.get("ssl_verify"), defaults.get("ssl_verify", True)),
        "headedMode": _as_bool(request.form.get("headed_mode"), defaults.get("headed_mode", False)),
        "base": (request.form.get("base") or defaults.get("base") or "acordaos").strip(),
        "synonym": _as_bool(request.form.get("synonym"), defaults.get("synonym", True)),
        "plural": _as_bool(request.form.get("plural"), defaults.get("plural", True)),
        "stems": _as_bool(request.form.get("stems"), defaults.get("stems", False)),
        "exactSearch": _as_bool(request.form.get("exact_search"), defaults.get("exact_search", True)),
        "processClassSigla": [
            s.strip().upper()
            for s in (request.form.get("process_class_sigla") or "").split(",")
            if s.strip()
        ],
        "dateStart": (request.form.get("date_start") or "").strip() or None,
        "dateEnd": (request.form.get("date_end") or "").strip() or None,
    }

    now = datetime.now(timezone.utc)
    doc = {
        "status": "scheduled",
        "scheduledFor": scheduled_for,
        "createdAt": now,
        "updatedAt": now,
        "query": query,
        "resultCount": None,
        "error": None,
        "executionMode": execution_mode,
        "source": "ui",
    }
    result = _get_scrape_jobs_collection().insert_one(doc)
    _web_log(
        "scraping.schedule",
        {
            "job_id": str(result.inserted_id),
            "execution_mode": execution_mode,
            "scheduled_for": scheduled_for.isoformat(),
            "query": query,
            "remote_addr": request.remote_addr,
        },
    )
    return redirect(url_for("scraping"))


@app.route("/scraping/cancel/<job_id>", methods=["POST"])
def scraping_cancel(job_id: str) -> Any:
    try:
        obj_id = ObjectId(job_id)
    except InvalidId:
        return redirect(url_for("scraping"))

    now = datetime.now(timezone.utc)
    result = _get_scrape_jobs_collection().update_one(
        {"_id": obj_id, "status": "scheduled"},
        {"$set": {"status": "canceled", "updatedAt": now}},
    )
    _web_log(
        "scraping.cancel",
        {
            "job_id": job_id,
            "matched": result.matched_count,
            "modified": result.modified_count,
            "remote_addr": request.remote_addr,
        },
    )
    return redirect(url_for("scraping"))


@app.route("/scraping/execute/<job_id>", methods=["POST"])
def scraping_execute(job_id: str) -> Any:
    try:
        obj_id = ObjectId(job_id)
    except InvalidId:
        return redirect(url_for("scraping"))

    jobs_col = _get_scrape_jobs_collection()
    job = jobs_col.find_one({"_id": obj_id})
    if not job:
        return redirect(url_for("scraping"))

    now = datetime.now(timezone.utc)
    run_id = str(uuid4())
    log_stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_path = str((BASE_DIR / "core" / "logs" / f"{log_stamp}-scrape-{run_id}.log"))
    jobs_col.update_one(
        {"_id": obj_id},
        {"$set": {
            "status": "running",
            "runId": run_id,
            "startedAt": now,
            "updatedAt": now,
            "logPath": log_path,
        }},
    )
    _web_log(
        "scraping.execute.start",
        {"job_id": job_id, "run_id": run_id, "log_path": log_path, "remote_addr": request.remote_addr},
    )

    query_raw = _load_query_raw()
    job_query = job.get("query") if isinstance(job.get("query"), dict) else {}
    merged = _merge_query_cfg(query_raw, job_query)

    temp_path = None
    try:
        with tempfile.NamedTemporaryFile("w", suffix=".yaml", delete=False, encoding="utf-8") as tmp:
            yaml.safe_dump(merged, tmp, allow_unicode=True, sort_keys=False)
            temp_path = tmp.name

        script_path = str(BASE_DIR / "core" / "step00-search-stf.py")
        log_file = Path(log_path)
        log_file.parent.mkdir(parents=True, exist_ok=True)
        with log_file.open("a", encoding="utf-8") as fh:
            fh.write(f"[{datetime.now().isoformat()}] START step00-search-stf.py runId={run_id}\n")
            result = subprocess.run(
                [sys.executable, script_path, "--query-config", temp_path],
                cwd=str(BASE_DIR / "core"),
                stdout=fh,
                stderr=fh,
                text=True,
            )
            fh.write(f"[{datetime.now().isoformat()}] END step00-search-stf.py rc={result.returncode}\n")

        latest_run = _get_case_query_collection().find_one(
            {"queryString": job_query.get("queryString")},
            sort=[("extractionTimestamp", -1)],
        )
        if latest_run:
            step01_path = str(BASE_DIR / "core" / "step01-extract-cases.py")
            with log_file.open("a", encoding="utf-8") as fh:
                fh.write(f"[{datetime.now().isoformat()}] START step01-extract-cases.py caseQueryId={latest_run.get('_id')}\n")
                step01_result = subprocess.run(
                    [sys.executable, step01_path, "--case-query-id", str(latest_run.get("_id"))],
                    cwd=str(BASE_DIR / "core"),
                    stdout=fh,
                    stderr=fh,
                    text=True,
                )
                fh.write(f"[{datetime.now().isoformat()}] END step01-extract-cases.py rc={step01_result.returncode}\n")
            _web_log(
                "scraping.execute.step01",
                {
                    "job_id": job_id,
                    "case_query_id": str(latest_run.get("_id")),
                    "return_code": step01_result.returncode,
                    "remote_addr": request.remote_addr,
                },
            )
        jobs_col.update_one(
            {"_id": obj_id},
            {
                "$set": {
                    "status": "completed" if result.returncode == 0 else "failed",
                    "updatedAt": datetime.now(timezone.utc),
                    "finishedAt": datetime.now(timezone.utc),
                    "caseQueryId": str(latest_run.get("_id")) if latest_run else None,
                    "resultCount": latest_run.get("extractedCount") if latest_run else None,
                    "lastError": None,
                }
            },
        )
        _web_log(
            "scraping.execute.finish",
            {
                "job_id": job_id,
                "run_id": run_id,
                "return_code": result.returncode,
                "latest_run_id": str(latest_run.get("_id")) if latest_run else None,
                "result_count": latest_run.get("extractedCount") if latest_run else None,
                "remote_addr": request.remote_addr,
            },
        )
    except Exception as e:
        jobs_col.update_one(
            {"_id": obj_id},
            {
                "$set": {
                    "status": "failed",
                    "updatedAt": datetime.now(timezone.utc),
                    "finishedAt": datetime.now(timezone.utc),
                    "error": str(e),
                }
            },
        )
        _web_log(
            "scraping.execute.error",
            {"job_id": job_id, "error": str(e), "remote_addr": request.remote_addr},
        )
    finally:
        if temp_path and os.path.exists(temp_path):
            os.unlink(temp_path)

    return redirect(url_for("scraping"))


@app.route("/scraping/<run_id>")
def scraping_detail(run_id: str) -> Any:
    try:
        obj_id = ObjectId(run_id)
    except InvalidId:
        return redirect(url_for("scraping"))

    case_query_col = _get_case_query_collection()
    case_query = case_query_col.find_one({"_id": obj_id})
    if not case_query:
        return redirect(url_for("scraping"))

    case_data_col = _get_collection()
    case_query_id_str = str(case_query.get("_id"))
    base_match = {"identity.caseQueryId": case_query_id_str}
    total_cases = case_data_col.count_documents(base_match)

    configured_steps = _load_pipeline_steps()
    steps = []
    for step in configured_steps:
        summary = _step_summary_for_script(case_query, case_data_col, base_match, step["script"])
        steps.append(
            {
                "name": step["script"],
                "status": summary["status"],
                "total": summary["total"],
                "success": summary.get("success", 0),
                "error": summary.get("error", 0),
                "remaining": summary.get("remaining", 0),
                "started_at": summary["started_at"],
                "finished_at": summary["finished_at"],
            }
        )

    status_meta = _status_meta()
    for step in steps:
        meta = status_meta.get(step["status"], status_meta["unknown"])
        step["status_label"] = meta["label"]
        step["status_class"] = meta["class"]
        step["started_at_fmt"] = _format_datetime(step["started_at"])
        step["finished_at_fmt"] = _format_datetime(step["finished_at"])

    detail = _build_query_detail(case_query)
    run_status = str(case_query.get("status") or "unknown")
    run_meta = status_meta.get(run_status, status_meta["unknown"])

    return render_template(
        "scraping_detail.html",
        title="CITO | Scraping | Detalhe",
        brand_sub="Scraping",
        hero_copy="Detalhes da execucao do scraping e etapas subsequentes da pipeline.",
        query_detail=detail,
        run=case_query,
        run_status=run_status,
        run_status_label=run_meta["label"],
        run_status_class=run_meta["class"],
        total_cases=total_cases,
        steps=steps,
        run_id=run_id,
    )


def _enqueue_pipeline_job(case_query_id: str, action: str) -> None:
    now = datetime.now(timezone.utc)
    _get_pipeline_jobs_collection().insert_one(
        {
            "caseQueryId": case_query_id,
            "action": action,
            "status": "scheduled",
            "createdAt": now,
            "updatedAt": now,
            "source": "ui",
        }
    )


@app.route("/scraping/<run_id>/pipeline/run", methods=["POST"])
def scraping_pipeline_run(run_id: str) -> Any:
    try:
        obj_id = ObjectId(run_id)
    except InvalidId:
        return redirect(url_for("scraping"))

    case_query_col = _get_case_query_collection()
    case_query = case_query_col.find_one({"_id": obj_id})
    if not case_query:
        return redirect(url_for("scraping"))

    core_dir = BASE_DIR / "core"
    pipeline_path = core_dir / "step00-run-pipeline-02-09.py"
    run_uuid = str(uuid4())
    log_stamp = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
    log_path = str((core_dir / "logs" / f"{log_stamp}-pipeline-{run_uuid}.log"))

    pipeline_jobs = _get_pipeline_jobs_collection()
    job_doc = {
        "caseQueryId": run_id,
        "action": "run",
        "status": "running",
        "runId": run_uuid,
        "logPath": log_path,
        "startedAt": datetime.now(timezone.utc),
        "updatedAt": datetime.now(timezone.utc),
        "source": "ui",
    }
    job_id = pipeline_jobs.insert_one(job_doc).inserted_id

    pipeline_result = subprocess.run(
        [sys.executable, str(pipeline_path), "--case-query-id", run_id, "--run-id", run_uuid],
        cwd=str(core_dir),
        capture_output=True,
        text=True,
    )
    _web_log(
        "pipeline.run.pipeline",
        {
            "case_query_id": run_id,
            "run_id": run_uuid,
            "return_code": pipeline_result.returncode,
            "remote_addr": request.remote_addr,
        },
    )
    pipeline_jobs.update_one(
        {"_id": job_id},
        {"$set": {
            "status": "completed" if pipeline_result.returncode == 0 else "failed",
            "finishedAt": datetime.now(timezone.utc),
            "updatedAt": datetime.now(timezone.utc),
        }},
    )
    _web_log(
        "pipeline.run.log_file",
        {
            "case_query_id": run_id,
            "run_id": run_uuid,
            "log_file": log_path,
            "remote_addr": request.remote_addr,
        },
    )
    return redirect(url_for("scraping_detail", run_id=run_id))


@app.route("/scraping/<run_id>/pipeline/reprocess", methods=["POST"])
def scraping_pipeline_reprocess(run_id: str) -> Any:
    _enqueue_pipeline_job(run_id, "reprocess")
    _web_log(
        "pipeline.reprocess.enqueue",
        {"case_query_id": run_id, "remote_addr": request.remote_addr},
    )
    return redirect(url_for("scraping_detail", run_id=run_id))


@app.route("/scraping/<run_id>/pipeline/cancel", methods=["POST"])
def scraping_pipeline_cancel(run_id: str) -> Any:
    now = datetime.now(timezone.utc)
    result = _get_pipeline_jobs_collection().update_many(
        {"caseQueryId": run_id, "status": {"$in": ["scheduled", "running"]}},
        {"$set": {"status": "canceled", "updatedAt": now}},
    )
    _web_log(
        "pipeline.cancel",
        {
            "case_query_id": run_id,
            "matched": result.matched_count,
            "modified": result.modified_count,
            "remote_addr": request.remote_addr,
        },
    )
    return redirect(url_for("scraping_detail", run_id=run_id))


@app.route("/processos")
def processos() -> Any:
    filters = _get_process_filters(request.args)
    collection = _get_collection()

    match = _build_process_match(filters)
    limit = _limit_value(request.args.get("limit"), default=25)
    total = collection.count_documents(match)
    rows = _fetch_processes(collection, match, limit=limit)
    kpis = _aggregate_process_kpis(collection, match)
    tag_limit = _limit_value(request.args.get("tag_limit"), default=20)
    tag_clouds = {
        "doctrine": _aggregate_doctrine_tag_cloud(collection, match, tag_limit),
        "legislation": _aggregate_legislation_tag_cloud(collection, match, tag_limit),
        "acordao": _aggregate_acordao_tag_cloud(collection, match, tag_limit),
    }

    class_options = _aggregate_case_classes(collection, {})
    rapporteur_options = _aggregate_rapporteur_options(collection)
    author_suggestions = _aggregate_author_suggestions(collection, limit=250)

    filter_params = {k: v for k, v in filters.items() if v}
    filter_params_no_author = {k: v for k, v in filter_params.items() if k != "author"}
    filter_params_no_legislation = {k: v for k, v in filter_params.items() if k != "legislation_norm"}
    filter_params_no_acordao = {k: v for k, v in filter_params.items() if k != "acordao_ref"}
    next_limit = limit + 50

    return render_template(
        "processos.html",
        title="CITO | Processos",
        brand_sub="Processos",
        hero_copy="Explore processos extraidos do STF com filtros avancados e acesso rapido aos detalhes.",
        filters=filters,
        filter_params=filter_params,
        class_options=class_options,
        rapporteur_options=rapporteur_options,
        author_suggestions=author_suggestions,
        processes=rows,
        total=total,
        kpis=kpis,
        tag_clouds=tag_clouds,
        tag_limit=tag_limit,
        filter_params_no_author=filter_params_no_author,
        filter_params_no_legislation=filter_params_no_legislation,
        filter_params_no_acordao=filter_params_no_acordao,
        limit=limit,
        next_limit=next_limit,
        show_more=total > limit,
    )


@app.route("/processos/kpis")
def processos_kpis() -> Any:
    filters = _get_process_filters(request.args)
    collection = _get_collection()
    match = _build_process_match(filters)
    kpis = _aggregate_process_kpis(collection, match)
    return {
        "totalProcesses": kpis["total_processes"],
        "processesPerRapporteur": kpis["processes_per_rapporteur"],
        "doctrinesPerCase": kpis["doctrines_per_case"],
        "legislationCitationsPerCase": kpis["legislation_per_case"],
        "meta": {
            "filtersApplied": {k: v for k, v in filters.items() if v},
            "computedAt": datetime.now(timezone.utc).isoformat(),
        },
    }


@app.route("/processos/<process_id>")
def processos_detail(process_id: str) -> Any:
    collection = _get_collection()
    detail = _fetch_process_detail(collection, process_id)
    if not detail:
        return redirect(url_for("processos"))

    return render_template(
        "processos_detail.html",
        title="CITO | Processos | Detalhe",
        brand_sub="Processos",
        hero_copy="Detalhamento completo do processo, doutrinas, palavras-chave e legislacao.",
        detail=detail,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000, use_reloader=False)
