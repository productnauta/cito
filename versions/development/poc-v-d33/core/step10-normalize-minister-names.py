#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step10-normalize-minister-names.py
Version: poc-v-d33      Date: 2026-02-04
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Normaliza nomes de ministros/relatores em registros existentes.
Inputs: config/mongo.yaml
Outputs: case_data atualizado (identity/caseIdentification/decisionDetails.ministerVotes).
Dependencies: pymongo, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from utils.mongo import get_case_data_collection
from utils.normalize import normalize_minister_name

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
CASE_DATA_COLLECTION = "case_data"


def _ts() -> str:
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


def _normalize_field(value: Any) -> Any:
    return normalize_minister_name(value) if isinstance(value, str) else value


def _normalize_votes(votes: Any) -> List[Dict[str, Any]]:
    if not isinstance(votes, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in votes:
        if not isinstance(item, dict):
            continue
        name = normalize_minister_name(item.get("ministerName") or item.get("minister"))
        new_item = dict(item)
        if name is not None:
            new_item["ministerName"] = name
        out.append(new_item)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description="Normaliza nomes de ministros/relatores em case_data.")
    parser.add_argument("--limit", type=int, default=0, help="Limita quantidade de documentos processados.")
    parser.add_argument("--dry-run", action="store_true", help="Não grava alterações no banco.")
    args = parser.parse_args()

    col = get_case_data_collection(MONGO_CONFIG_PATH, CASE_DATA_COLLECTION)
    cursor = col.find({}, projection={
        "identity.rapporteur": 1,
        "identity.opinionWriter": 1,
        "caseIdentification.rapporteur": 1,
        "caseIdentification.opinionWriter": 1,
        "caseData.decisionDetails.ministerVotes": 1,
    })

    total = 0
    updated = 0
    for doc in cursor:
        if args.limit and total >= args.limit:
            break
        total += 1
        doc_id = doc.get("_id")
        identity = doc.get("identity") or {}
        case_ident = doc.get("caseIdentification") or {}
        decision_details = (doc.get("caseData") or {}).get("decisionDetails") or {}

        updates: Dict[str, Any] = {}

        for key in ("rapporteur", "opinionWriter"):
            if key in identity and isinstance(identity.get(key), str):
                norm = _normalize_field(identity.get(key))
                if norm and norm != identity.get(key):
                    updates[f"identity.{key}"] = norm

        for key in ("rapporteur", "opinionWriter"):
            if key in case_ident and isinstance(case_ident.get(key), str):
                norm = _normalize_field(case_ident.get(key))
                if norm and norm != case_ident.get(key):
                    updates[f"caseIdentification.{key}"] = norm

        votes = decision_details.get("ministerVotes")
        if isinstance(votes, list):
            normalized_votes = _normalize_votes(votes)
            if normalized_votes != votes:
                updates["caseData.decisionDetails.ministerVotes"] = normalized_votes

        if updates:
            updates["audit.updatedAt"] = datetime.now(timezone.utc)
            if not args.dry_run:
                col.update_one({"_id": doc_id}, {"$set": updates})
            updated += 1

    log(f"Processados: {total} | Atualizados: {updated} | Dry-run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
