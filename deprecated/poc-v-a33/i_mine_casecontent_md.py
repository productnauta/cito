#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
i_mine_casecontent_md.py

Minera dados a partir de caseContent.contentMd (Markdown) em case_data:
- Identifica seções com títulos ####
- Extrai conteúdo para caseData.* conforme mapeamento
- Partes: linha a linha, separa tipo/nome pelo ':'
- Indexação: palavras-chave separadas por vírgula
- Títulos não mapeados: salva em caseData.<titulo>

Dependências:
pip install pymongo
"""

from __future__ import annotations

import os
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection


# =========================
# Mongo (fixo)
# =========================
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"
COLLECTION = "case_data"


def _env_bool(name: str, default: bool) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return v.strip().lower() in ("1", "true", "yes", "y", "on", "sim", "s")


FORCE_REPROCESS = _env_bool("FORCE_REPROCESS", False)
LIMIT = int(os.getenv("LIMIT", "0") or "0")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


def _clean_line(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _safe_field_name(title: str) -> str:
    name = (title or "").strip()
    name = name.replace(".", "_").replace("$", "_")
    return name


def parse_sections(md: str) -> Dict[str, str]:
    sections: Dict[str, List[str]] = {}
    current_title: Optional[str] = None

    for raw_line in (md or "").splitlines():
        line = raw_line.rstrip()
        m = re.match(r"^####\s+(.*)$", line)
        if m:
            current_title = _clean_line(m.group(1))
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
        ln = _clean_line(line)
        if not ln:
            continue
        if ":" in ln:
            left, right = ln.split(":", 1)
            p_type = _clean_line(left)
            p_name = _clean_line(right)
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
    parts = [p.strip() for p in raw.split(",")]
    return [p for p in parts if p]


def build_case_data(sections: Dict[str, str]) -> Dict[str, Any]:
    mapped: Dict[str, Any] = {}

    for title, content in sections.items():
        if title == "Publicação":
            mapped["casePublication"] = content
        elif title == "Partes":
            parties = parse_parties(content)
            if parties:
                mapped["caseParties"] = parties
        elif title == "Ementa":
            mapped["caseSummary"] = content
        elif title == "Decisão":
            mapped["caseDecision"] = content
        elif title == "Indexação":
            keywords = parse_keywords(content)
            if keywords:
                mapped["caseKeywords"] = keywords
        elif title == "Legislação":
            mapped["caseLegislation"] = content
        elif title == "Observação":
            mapped["caseNotes"] = content
        elif title == "Doutrina":
            mapped["caseDoctrine"] = content
        else:
            safe = _safe_field_name(title)
            if safe:
                mapped[safe] = content

    return mapped


def claim_next_doc(col: Collection) -> Optional[Dict[str, Any]]:
    base_filter: Dict[str, Any] = {
        "caseContent.contentMd": {"$exists": True, "$ne": ""},
        "processing.caseContentMineStatus": {"$ne": "processing"},
    }
    if not FORCE_REPROCESS:
        base_filter["processing.caseContentMinedAt"] = {"$exists": False}

    return col.find_one_and_update(
        base_filter,
        {"$set": {"processing.caseContentMineStatus": "processing", "processing.caseContentMiningAt": utc_now()}},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_success(col: Collection, doc_id, case_data: Dict[str, Any]) -> None:
    update_fields: Dict[str, Any] = {
        "processing.caseContentMinedAt": utc_now(),
        "processing.caseContentMineStatus": "done",
        "processing.caseContentMineError": None,
    }

    for key, value in case_data.items():
        update_fields[f"caseData.{key}"] = value

    col.update_one({"_id": doc_id}, {"$set": update_fields})


def mark_error(col: Collection, doc_id, error_msg: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "processing.caseContentMinedAt": utc_now(),
            "processing.caseContentMineStatus": "error",
            "processing.caseContentMineError": error_msg,
        }},
    )


def main() -> int:
    col = get_collection()
    processed = 0

    while True:
        if LIMIT and processed >= LIMIT:
            break

        doc = claim_next_doc(col)
        if not doc:
            break

        doc_id = doc.get("_id")
        title = (doc.get("caseTitle") or "Sem título").strip()

        try:
            md = (doc.get("caseContent", {}) or {}).get("contentMd") or ""
            md = md.strip()
            if not md:
                raise ValueError("Campo caseContent.contentMd vazio.")

            sections = parse_sections(md)
            if not sections:
                raise ValueError("Nenhuma seção #### encontrada no contentMd.")

            case_data = build_case_data(sections)
            if not case_data:
                raise ValueError("Nenhum dado mapeado a partir das seções.")

            mark_success(col, doc_id, case_data)
            processed += 1
            print(f"OK: {doc_id} - {title}")

        except Exception as e:
            mark_error(col, doc_id, str(e))
            print(f"ERRO: {doc_id} - {title}: {e}")

    print(f"Processamento finalizado. Total: {processed}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
