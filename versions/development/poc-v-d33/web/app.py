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
from datetime import date, datetime, timedelta
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
DECISION_DETAILS_PATH = "caseData.decisionDetails"
MINISTER_VOTES_PATH = f"{DECISION_DETAILS_PATH}.ministerVotes"
DECISION_RESULT_PATH = f"{DECISION_DETAILS_PATH}.decisionResult.finalDecision"
CITATIONS_PATH = f"{DECISION_DETAILS_PATH}.citations"

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


def _parse_date_value(value: str) -> Optional[date]:
    if not value:
        return None
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError:
        return None


def _get_filters(args: Dict[str, Any]) -> Dict[str, str]:
    return {
        "author": str(args.get("author") or "").strip(),
        "title": str(args.get("title") or "").strip(),
        "case_class": str(args.get("case_class") or "").strip(),
        "judgment_year": str(args.get("judgment_year") or "").strip(),
        "rapporteur": str(args.get("rapporteur") or "").strip(),
        "judging_body": str(args.get("judging_body") or "").strip(),
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


def _aggregate_ministers(collection: Collection, filters: Dict[str, str]) -> List[Dict[str, Any]]:
    case_match = _build_ministro_case_match(filters)
    minister_value = filters.get("minister") or ""
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
    rapporteur_pipeline.append(
        {
            "$addFields": {
                "_rapporteur": {
                    "$ifNull": ["$identity.rapporteur", "$caseIdentification.rapporteur"]
                }
            }
        }
    )
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
        name = row["label"]
        stats[name] = {
            "minister": name,
            "case_ids": set(row.get("case_ids") or []),
            "total_relatorias": row.get("total_relatorias") or 0,
            "citations_total": 0,
            "total_votes_defined": 0,
            "total_votes_pending": 0,
            "total_votes_vencido": 0,
        }

    for row in collection.aggregate(vote_pipeline):
        name = row["label"]
        entry = stats.get(name)
        if not entry:
            entry = {
                "minister": name,
                "case_ids": set(),
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
        name = row["label"]
        entry = stats.get(name)
        if not entry:
            entry = {
                "minister": name,
                "case_ids": set(),
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

    relatoria_match = {
        "$and": [
            _build_ministro_case_match(filters) or {},
            {
                "$or": [
                    {"identity.rapporteur": _regex(minister_name, exact=True)},
                    {"caseIdentification.rapporteur": _regex(minister_name, exact=True)},
                ]
            },
        ]
    }
    relatoria_match = {
        "$and": [clause for clause in relatoria_match["$and"] if clause]
    }
    total_relatorias = _count_distinct_cases(collection, relatoria_match)

    citations_pipeline = [
        {"$match": match},
        {"$addFields": {"_citationsCount": _citations_count_expr(all_citation_types)}},
        {"$group": {"_id": None, "total": {"$sum": "$_citationsCount"}}},
    ]
    citations_result = list(collection.aggregate(citations_pipeline))
    total_citations = int(citations_result[0]["total"]) if citations_result else 0

    decision_pipeline = [
        {"$match": match},
        {
            "$addFields": {
                "_finalDecision": {
                    "$ifNull": [f"${DECISION_RESULT_PATH}", "Não informado"]
                }
            }
        },
        {"$group": {"_id": "$_finalDecision", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "total": 1}},
        {"$sort": {"total": -1, "label": 1}},
    ]
    decision_distribution = list(collection.aggregate(decision_pipeline))

    doctrine_pipeline = [
        {"$match": match},
        {"$unwind": f"${DOCTRINE_PATH}"},
        {"$match": {f"{DOCTRINE_PATH}.author": {"$nin": [None, ""]}}},
        {"$group": {"_id": f"${DOCTRINE_PATH}.author", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "total": 1}},
        {"$sort": {"total": -1, "label": 1}},
        {"$limit": 5},
    ]
    top_doctrine = list(collection.aggregate(doctrine_pipeline))

    norms_pipeline = [
        {"$match": match},
        {"$unwind": f"${CITATIONS_PATH}"},
        {
            "$match": {
                f"{CITATIONS_PATH}.citationType": {"$in": norm_citation_types},
                f"{CITATIONS_PATH}.citationName": {"$nin": [None, ""]},
            }
        },
        {"$group": {"_id": f"${CITATIONS_PATH}.citationName", "total": {"$sum": 1}}},
        {"$project": {"_id": 0, "label": "$_id", "total": 1}},
        {"$sort": {"total": -1, "label": 1}},
        {"$limit": 5},
    ]
    top_norms = list(collection.aggregate(norms_pipeline))

    return {
        "total_processes": total_processes,
        "total_relatorias": total_relatorias,
        "total_citations": total_citations,
        "decision_distribution": decision_distribution,
        "top_doctrine": top_doctrine,
        "top_norms": top_norms,
    }

app = Flask(__name__)


@app.route("/")
def index() -> Any:
    return redirect(url_for("doutrina"))


@app.route("/doutrina")
def doutrina() -> Any:
    filters = _get_filters(request.args)
    collection = _get_collection()

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

    return render_template(
        "doutrina.html",
        title="CITO | Doutrina",
        filters=filters,
        filter_params=filter_params,
        summary_total=summary_total,
        authors=authors,
        titles=titles,
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

    match = _build_match(filters, overrides=overrides)
    collection = _get_collection()
    limit = _limit_value(request.args.get("limit"), default=10)
    total_cases = collection.count_documents(match)
    cases = _fetch_cases(collection, match, limit=limit)

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

    filter_params = {k: v for k, v in filters.items() if v}
    filter_params_no_minister = {k: v for k, v in filter_params.items() if k != "minister"}

    return render_template(
        "ministro_detail.html",
        title="CITO | Ministros | Detalhe",
        minister_name=minister_name,
        filter_params=filter_params,
        filter_params_no_minister=filter_params_no_minister,
        total_processes=details["total_processes"],
        total_relatorias=details["total_relatorias"],
        total_citations=details["total_citations"],
        decision_distribution=details["decision_distribution"],
        top_doctrine=details["top_doctrine"],
        top_norms=details["top_norms"],
    )


if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5000)
