#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step11-normalize-work-titles.py
Version: poc-v-d33      Date: 2026-02-04
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Normaliza titulos de obras em registros existentes (workKey/normTitle/displayTitle).
Inputs: config/mongo.yaml, config/work_aliases.yaml
Outputs: case_data atualizado (caseData.doctrineReferences.*).
Dependencies: pymongo, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

from utils.mongo import get_case_data_collection
from utils.work_normalize import canonicalize_work, load_alias_map

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
WORK_ALIAS_PATH = CONFIG_DIR / "work_aliases.yaml"
CASE_DATA_COLLECTION = "case_data"


def _ts() -> str:
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


def _normalize_refs(refs: Any, alias_map: Dict[str, str]) -> Tuple[List[Dict[str, Any]], bool]:
    if not isinstance(refs, list):
        return [], False
    output: List[Dict[str, Any]] = []
    changed = False
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        raw_title = str(ref.get("publicationTitle") or "").strip()
        canonical = canonicalize_work(raw_title, alias_map)
        new_ref = dict(ref)
        updates = {
            "publicationTitleRaw": raw_title,
            "publicationTitleNorm": canonical.get("normTitle") or "",
            "workKey": canonical.get("workKey") or "",
            "workMatchType": canonical.get("matchType") or "normalized",
            "publicationTitleDisplay": canonical.get("displayTitle") or raw_title,
        }
        for key, value in updates.items():
            if new_ref.get(key) != value:
                new_ref[key] = value
                changed = True
        output.append(new_ref)
    return output, changed


def main() -> int:
    parser = argparse.ArgumentParser(description="Normaliza titulos de obras em case_data.")
    parser.add_argument("--limit", type=int, default=0, help="Limita quantidade de documentos processados.")
    parser.add_argument("--dry-run", action="store_true", help="Nao grava alteracoes no banco.")
    args = parser.parse_args()

    col = get_case_data_collection(MONGO_CONFIG_PATH, CASE_DATA_COLLECTION)
    alias_map = load_alias_map(WORK_ALIAS_PATH)
    cursor = col.find(
        {"caseData.doctrineReferences": {"$exists": True}},
        projection={"caseData.doctrineReferences": 1},
    )

    total = 0
    updated = 0
    for doc in cursor:
        if args.limit and total >= args.limit:
            break
        total += 1
        doc_id = doc.get("_id")
        refs = (doc.get("caseData") or {}).get("doctrineReferences") or []
        new_refs, changed = _normalize_refs(refs, alias_map)
        if changed:
            if not args.dry_run:
                col.update_one(
                    {"_id": doc_id},
                    {
                        "$set": {
                            "caseData.doctrineReferences": new_refs,
                            "audit.updatedAt": datetime.now(timezone.utc),
                        }
                    },
                )
            updated += 1

    log(f"Processados: {total} | Atualizados: {updated} | Dry-run={args.dry_run}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
