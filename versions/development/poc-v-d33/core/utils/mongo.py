#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: utils/mongo.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Utilitario para carregar configuracao YAML e abrir conexao MongoDB.
Inputs: config/mongo.yaml
Outputs: MongoClient / Collection prontos para uso.
Dependencies: pymongo, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

import yaml
from pymongo import MongoClient
from pymongo.collection import Collection


def _ts() -> str:
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


def load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config invalida: chave 'mongo' ausente ou invalida.")
    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri:
        raise ValueError("Config invalida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config invalida: 'mongo.database' vazio.")
    return MongoCfg(uri=uri, database=db)


def get_case_data_collection(config_path: Path, collection_name: str = "case_data") -> Collection:
    log(f"Lendo config MongoDB: {config_path}")
    raw = load_yaml(config_path)
    cfg = build_mongo_cfg(raw)

    log("Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log(f"MongoDB OK | db='{cfg.database}' | collection='{collection_name}'")
    return client[cfg.database][collection_name]


def get_mongo_client(config_path: Path) -> tuple[MongoClient, str]:
    """
    Retorna MongoClient e nome do database, a partir do mongo.yaml.
    """
    log(f"Lendo config MongoDB: {config_path}")
    raw = load_yaml(config_path)
    cfg = build_mongo_cfg(raw)
    log("Conectando ao MongoDB")
    client = MongoClient(cfg.uri)
    log(f"MongoDB OK | db='{cfg.database}'")
    return client, cfg.database
