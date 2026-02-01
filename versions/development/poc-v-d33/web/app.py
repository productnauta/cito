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

import re
import sys
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, redirect, render_template, request, url_for
from pymongo.collection import Collection


BASE_DIR = Path(__file__).resolve().parents[1]
CORE_DIR = BASE_DIR / "core"
if str(CORE_DIR) not in sys.path:
    sys.path.insert(0, str(CORE_DIR))

from utils.mongo import get_case_data_collection


CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
COLLECTION_NAME = "case_data"
DOCTRINE_PATH = "caseData.doctrineReferences"

_collection: Optional[Collection] = None


def _get_collection() -> Collection:
    global _collection
    if _collection is None:
        _collection = get_case_data_collection(MONGO_CONFIG_PATH, COLLECTION_NAME)
    return _collection


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


def _get_filters(args: Dict[str, Any]) -> Dict[str, str]:
    return {
        "author": str(args.get("author") or "").strip(),
        "title": str(args.get("title") or "").strip(),
        "case_class": str(args.get("case_class") or "").strip(),
        "judgment_year": str(args.get("judgment_year") or "").strip(),
        "rapporteur": str(args.get("rapporteur") or "").strip(),
        "judging_body": str(args.get("judging_body") or "").strip(),
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

    case_class = filters.get("case_class") or ""
    judging_body = filters.get("judging_body") or ""
    year_raw = filters.get("judgment_year") or ""

    if case_class:
        rx = _regex(case_class)
        and_clauses.append(
            {
                "$or": [
                    {"identity.caseClassDetail": rx},
                    {"caseIdentification.caseClassDetail": rx},
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
                "cases": {"$addToSet": _case_id_expr()},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": {"$size": "$cases"}}})
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
                "cases": {"$addToSet": _case_id_expr()},
            }
        }
    )
    pipeline.append({"$project": {"_id": 0, "label": "$_id", "total": {"$size": "$cases"}}})
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


def _fetch_cases(collection: Collection, match: Dict[str, Any]) -> List[Dict[str, Any]]:
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
    }
    cursor = collection.find(match, projection=projection).sort("dates.judgmentDate", -1)
    cases: List[Dict[str, Any]] = []
    for doc in cursor:
        identity = doc.get("identity") or {}
        case_ident = doc.get("caseIdentification") or {}
        dates = doc.get("dates") or {}
        case_content = doc.get("caseContent") or {}

        case_title = identity.get("caseTitle") or doc.get("caseTitle") or "-"
        stf_id = identity.get("stfDecisionId") or str(doc.get("_id"))
        rapporteur = identity.get("rapporteur") or case_ident.get("rapporteur") or "-"
        judging_body = identity.get("judgingBody") or case_ident.get("judgingBody") or "-"
        case_url = case_content.get("caseUrl") or identity.get("caseUrl") or ""
        judgment_date = _format_date(dates.get("judgmentDate") or identity.get("judgmentDate"))

        cases.append(
            {
                "case_title": case_title,
                "stf_id": stf_id,
                "case_url": case_url,
                "judgment_date": judgment_date,
                "rapporteur": rapporteur,
                "judging_body": judging_body,
            }
        )
    return cases


app = Flask(__name__)


@app.route("/")
def index() -> Any:
    return redirect(url_for("doutrina"))


@app.route("/doutrina")
def doutrina() -> Any:
    filters = _get_filters(request.args)
    collection = _get_collection()

    summary_total = _count_cases(collection, filters)
    authors = _aggregate_authors(collection, filters)
    titles = _aggregate_titles(collection, filters)
    rapporteurs = _aggregate_rapporteurs(collection, filters)

    filter_params = {k: v for k, v in filters.items() if v}

    return render_template(
        "doutrina.html",
        title="CITO | Doutrina",
        filters=filters,
        filter_params=filter_params,
        summary_total=summary_total,
        authors=authors,
        titles=titles,
        rapporteurs=rapporteurs,
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

    match = _build_match(filters, overrides=overrides)
    collection = _get_collection()
    cases = _fetch_cases(collection, match)

    filter_params = {k: v for k, v in filters.items() if v}

    return render_template(
        "doutrina_detail.html",
        title="CITO | Doutrina | Detail",
        detail_kind=label_map.get(kind, kind).upper(),
        detail_value=value,
        total=len(cases),
        cases=cases,
        filter_params=filter_params,
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
