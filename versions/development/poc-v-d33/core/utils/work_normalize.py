#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: work_normalize.py
Version: poc-v-d33      Date: 2026-02-04
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Normaliza titulos de obras e gera workKey canonico com suporte a alias.
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import hashlib
import re
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

_WS_RE = re.compile(r"\s+")
_TRAIL_PUNCT_RE = re.compile(r"[.;:]+$")
_QUOTE_RE = re.compile(r"[\"'\[\]\(\)]")
_DASH_RE = re.compile(r"[–—]")

_DESCRIPTIVE_PREFIXES = (
    "promulgada",
    "rev",
    "rev.",
    "revista",
    "atual",
    "atualizada",
    "comentada",
    "anotada",
    "com",
    "coord",
    "coord.",
    "coordenado",
    "coordenada",
    "org",
    "org.",
    "organizado",
    "organizada",
    "ed",
    "ed.",
    "edição",
    "edicao",
    "volume",
    "vol",
    "vol.",
    "tomo",
)

_DATE_LONG_RE = re.compile(r"\b\d{1,2}\s+de\s+\w+\s+de\s+\d{4}\b")
_DATE_SHORT_RE = re.compile(r"\b\d{2}/\d{2}/\d{4}\b")

_NOISE_RE = re.compile(
    r"\b("
    r"rev\.?|revista|atual(?:izada)?|comentada|anotada|"
    r"coord\.?|coordenador(?:es|a)?|org\.?|organizado(?:ra)?|"
    r"vol\.?|volume|tomo|ed\.?|edi[cç][aã]o|"
    r"\d+a"
    r")\b",
    flags=re.IGNORECASE,
)


def title_case(value: str) -> str:
    return " ".join(part.capitalize() for part in value.split())


def normalize_title(raw_title: Any) -> str:
    if raw_title is None:
        return ""
    s = str(raw_title).strip()
    if not s:
        return ""

    s = unicodedata.normalize("NFKC", s)
    s = _WS_RE.sub(" ", s)
    s = _DASH_RE.sub("-", s)
    s = _QUOTE_RE.sub("", s)
    s = s.strip()
    s = _TRAIL_PUNCT_RE.sub("", s)

    s_lower = s.lower()

    if ":" in s_lower:
        before, after = s_lower.split(":", 1)
        after_strip = after.strip()
        if (
            after_strip.startswith(_DESCRIPTIVE_PREFIXES)
            or _DATE_LONG_RE.search(after_strip)
            or _DATE_SHORT_RE.search(after_strip)
        ):
            s_lower = before.strip()

    s_lower = _DATE_LONG_RE.sub("", s_lower)
    s_lower = _DATE_SHORT_RE.sub("", s_lower)

    cleaned = _NOISE_RE.sub(" ", s_lower)
    cleaned = _WS_RE.sub(" ", cleaned).strip()
    if len(cleaned) >= 8:
        s_lower = cleaned

    s_lower = s_lower.replace("-", " ")
    s_lower = _WS_RE.sub(" ", s_lower).strip()
    return s_lower


def _hash_work(norm_title: str) -> str:
    return hashlib.sha1(f"work:{norm_title}".encode("utf-8")).hexdigest()


def load_alias_map(path: Path) -> Dict[str, str]:
    if not path.exists():
        return {}
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    works = raw.get("works") if isinstance(raw.get("works"), list) else []
    alias_map: Dict[str, str] = {}
    for item in works:
        if not isinstance(item, dict):
            continue
        canonical = str(item.get("canonical") or "").strip()
        if not canonical:
            continue
        variants = item.get("variants") if isinstance(item.get("variants"), list) else []
        keys = [canonical] + [str(v) for v in variants if v]
        for variant in keys:
            norm = normalize_title(variant)
            if norm:
                alias_map[norm] = canonical
    return alias_map


def canonicalize_work(
    raw_title: Any,
    alias_map: Dict[str, str],
    fuzzy_enabled: bool = False,
    fuzzy_threshold: float = 0.9,
    catalog: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, str]:
    norm = normalize_title(raw_title)
    if not norm:
        return {
            "normTitle": "",
            "workKey": "",
            "displayTitle": "",
            "matchType": "normalized",
        }

    if norm in alias_map:
        canonical_title = alias_map[norm]
        canonical_norm = normalize_title(canonical_title)
        return {
            "normTitle": canonical_norm,
            "workKey": _hash_work(canonical_norm),
            "displayTitle": canonical_title,
            "matchType": "alias",
        }

    # Fuzzy opcional (nao habilitado por padrao)
    if fuzzy_enabled and catalog:
        best = None
        for entry in catalog:
            candidate = str(entry.get("canonicalNorm") or "")
            if not candidate:
                continue
            # fallback simples de similaridade: jaccard por tokens
            tokens_a = {t for t in norm.split() if len(t) > 1}
            tokens_b = {t for t in candidate.split() if len(t) > 1}
            if not tokens_a or not tokens_b:
                continue
            inter = tokens_a.intersection(tokens_b)
            union = tokens_a.union(tokens_b)
            sim = len(inter) / len(union) if union else 0
            if sim >= fuzzy_threshold:
                if best is None or sim > best["sim"]:
                    best = {"sim": sim, "workKey": entry.get("workKey"), "canonicalNorm": candidate}
        if best and best.get("workKey"):
            return {
                "normTitle": best["canonicalNorm"],
                "workKey": str(best["workKey"]),
                "displayTitle": title_case(best["canonicalNorm"]),
                "matchType": "fuzzy",
            }

    return {
        "normTitle": norm,
        "workKey": _hash_work(norm),
        "displayTitle": title_case(str(raw_title or "").strip()) or str(raw_title or ""),
        "matchType": "normalized",
    }

