#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: utils/normalize.py
Version: poc-v-d33      Date: 2026-02-04
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Utilitários de normalização determinísticos (nomes de ministros/relatores).
Inputs: strings
Outputs: strings normalizadas
Dependencies: re
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import re
from typing import Optional


_MIN_PREFIX_RE = re.compile(r"\bmin\.?\b", flags=re.IGNORECASE)


def normalize_minister_name(value: Optional[str]) -> Optional[str]:
    """
    Normaliza nomes de ministros/relatores:
    - remove "Min"/"Min." (case-insensitive)
    - colapsa espaços
    - aplica Title Case por token
    """
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    s = _MIN_PREFIX_RE.sub("", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return None
    return s.title()
