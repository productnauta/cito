#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step05-extract-keywords-parties.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts parties and keywords from Markdown sections into structured caseData fields.
Inputs: config/mongo.yaml, caseContent.md.parties, caseContent.md.keywords.
Outputs: caseData.caseParties and caseData.caseKeywords; processing/status updates.
Pipeline: parse parties -> parse keywords -> persist structured fields.
Dependencies: pymongo
------------------------------------------------------------

"""

from __future__ import annotations

import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from pymongo.collection import Collection
from pymongo.errors import PyMongoError

from utils.mongo import get_case_data_collection

# =============================================================================
# 0) LOG / TIME
# =============================================================================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


# =============================================================================
# 1) CONFIG (Mongo)
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"

CASE_DATA_COLLECTION = "case_data"

OUTPUT_PIPELINE_STATUS = "casePartiesKeywordsExtracted"
ERROR_PIPELINE_STATUS = "casePartiesKeywordsExtractError"


def get_case_data_collection_local() -> Collection:
    return get_case_data_collection(MONGO_CONFIG_PATH, CASE_DATA_COLLECTION)


# =============================================================================
# 2) PARSERS
# =============================================================================

PARTY_LINE_RE = re.compile(r"^\s*(?P<ptype>[^:]{1,120})\s*:\s*(?P<pname>.+?)\s*$")


def normalize_md_text(s: str) -> str:
    s = (s or "").replace("\r", "\n")
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()


def parse_parties_from_md(md_text: str) -> List[Dict[str, str]]:
    md_text = normalize_md_text(md_text)
    if not md_text:
        return []

    parties: List[Dict[str, str]] = []
    seen = set()

    for raw_line in md_text.split("\n"):
        line = re.sub(r"^[-*•]+\s*", "", raw_line.strip())
        if not line or line.startswith("#"):
            continue

        m = PARTY_LINE_RE.match(line)
        if not m:
            continue

        ptype = re.sub(r"\s+", " ", m.group("ptype")).strip()
        pname = re.sub(r"\s+", " ", m.group("pname")).strip()

        key = (ptype.lower(), pname.lower())
        if key in seen:
            continue
        seen.add(key)

        parties.append({
            "partieType": ptype,
            "partieName": pname,
        })

    return parties


def parse_keywords_from_md(md_text: str) -> List[str]:
    md_text = normalize_md_text(md_text)
    if not md_text:
        return []

    blob = re.sub(r"^[-*•]+\s*", "", md_text, flags=re.MULTILINE)
    parts = [p.strip() for p in blob.split(",")]

    keywords: List[str] = []
    seen = set()
    for p in parts:
        p = re.sub(r"\s+", " ", p).strip()
        if not p:
            continue
        low = p.lower()
        if low in seen:
            continue
        seen.add(low)
        keywords.append(p)

    return keywords


# =============================================================================
# 3) PROCESSAMENTO ÚNICO
# =============================================================================

def process_document(col: Collection, stf_decision_id: str) -> int:
    log("STEP", f"Buscando documento identity.stfDecisionId='{stf_decision_id}'")

    doc = col.find_one({"identity.stfDecisionId": stf_decision_id})
    if not doc:
        log("ERROR", "Documento não encontrado.")
        return 1

    doc_id = doc.get("_id")
    md_node = (doc.get("caseContent") or {}).get("md") or {}

    md_parties = md_node.get("parties") or ""
    md_keywords = md_node.get("keywords") or ""

    log("STEP", "Extraindo partes envolvidas")
    parties = parse_parties_from_md(md_parties)

    log("STEP", "Extraindo palavras-chave")
    keywords = parse_keywords_from_md(md_keywords)

    update = {
        "caseData.caseParties": parties,
        "caseData.caseKeywords": keywords,
        "processing.partiesKeywords": {
            "finishedAt": utc_now(),
            "partiesCount": len(parties),
            "keywordsCount": len(keywords),
        },
        "processing.partiesKeywordsStatus": "success",
        "processing.pipelineStatus": OUTPUT_PIPELINE_STATUS,
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": OUTPUT_PIPELINE_STATUS,
        "status.updatedAt": utc_now(),
    }

    col.update_one({"_id": doc_id}, {"$set": update})

    log("OK", f"Processamento concluído | partes={len(parties)} | keywords={len(keywords)}")
    return 0


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "ETAPA: EXTRAIR PARTES ENVOLVIDAS E PALAVRAS-CHAVE")

    try:
        col = get_case_data_collection_local()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId não informado.")
        return 1

    try:
        return process_document(col, stf_decision_id)
    except Exception as e:
        log("ERROR", f"Erro fatal: {type(e).__name__}: {e}")
        col.update_one(
            {"identity.stfDecisionId": stf_decision_id},
            {"$set": {
                "processing.pipelineStatus": ERROR_PIPELINE_STATUS,
                "processing.partiesKeywordsStatus": "error",
                "status.pipelineStatus": ERROR_PIPELINE_STATUS,
                "status.error": str(e),
                "status.updatedAt": utc_now(),
            }},
        )
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("WARN", "Interrompido pelo usuário.")
        sys.exit(130)
    except PyMongoError as e:
        log("ERROR", f"MongoDB erro: {e}")
        sys.exit(2)
