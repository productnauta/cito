#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
f-extract-case-sections.py

Objetivo:
- Ler HTML sanitizado em case_data.caseContent.caseHtmlClean
- Extrair secoes (header/publicacao/partes/ementa/decisao/indexacao/legislacao/observacao/doutrina)
- Persistir em caseContent.raw*
- Atualizar processing/audit/status

Dependencias:
  pip install pymongo beautifulsoup4
"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


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
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"

CASE_DATA_COLLECTION = "case_data"


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


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


def get_case_data_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("STEP", "Validando conexao (ping)")
    client.admin.command("ping")

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) EXTRACAO DE SECOES
# =============================================================================

SECTION_TITLES = {
    "Publicacao": "caseContent.rawPublication",
    "Partes": "caseContent.rawParties",
    "Ementa": "caseContent.rawSummary",
    "Decisao": "caseContent.rawDecision",
    "Indexacao": "caseContent.rawKeywords",
    "Legislacao": "caseContent.rawLegislation",
    "Observacao": "caseContent.rawNotes",
    "Doutrina": "caseContent.rawDoctrine",
}


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _norm_title(s: str) -> str:
    # Remove acentos mais comuns apenas para comparacao basica
    s = _norm_space(s)
    s = s.replace("\u00e7", "c").replace("\u00e3", "a").replace("\u00e1", "a").replace("\u00e9", "e")
    s = s.replace("\u00ed", "i").replace("\u00f3", "o").replace("\u00fa", "u")
    return s


def extract_sections(case_html_clean: str) -> Tuple[Dict[str, str], Dict[str, Any]]:
    """
    Extrai secoes com base em div.jud-text > h4 + div.
    Retorna (sections, meta).
    """
    sections: Dict[str, str] = {}
    meta: Dict[str, Any] = {"found": [], "missing": []}

    if not case_html_clean or not case_html_clean.strip():
        return sections, meta

    soup = BeautifulSoup(case_html_clean, "html.parser")

    # Indexa por titulo
    for block in soup.find_all("div", class_="jud-text"):
        h4 = block.find("h4")
        if not h4:
            continue

        title = _norm_space(h4.get_text(" ", strip=True))
        if not title:
            continue

        norm_title = _norm_title(title)
        for desired in list(SECTION_TITLES.keys()):
            if _norm_title(desired) == norm_title:
                content_div = h4.find_next_sibling("div")
                if content_div is not None:
                    sections[SECTION_TITLES[desired]] = content_div.decode_contents().strip()
                else:
                    sections[SECTION_TITLES[desired]] = ""
                break

    # Header (identificacao principal) - primeiro jud-text que contenha h4 com ADI
    header_html = None
    for block in soup.find_all("div", class_="jud-text"):
        h4 = block.find("h4")
        if not h4:
            continue
        h4_text = _norm_space(h4.get_text(" ", strip=True))
        if "ADI" in h4_text:
            header_html = block.decode_contents().strip()
            break

    if header_html:
        sections["caseContent.rawHeader"] = header_html

    # Meta de encontrados
    for title, field in SECTION_TITLES.items():
        if field in sections and sections[field] is not None:
            meta["found"].append(title)
        else:
            meta["missing"].append(title)

    if "caseContent.rawHeader" in sections:
        meta["found"].append("Header")
    else:
        meta["missing"].append("Header")

    return sections, meta


# =============================================================================
# 3) UPDATE HELPERS
# =============================================================================

def persist_success(col: Collection, doc_id: Any, *, sections: Dict[str, str], meta: Dict[str, Any]) -> None:
    update: Dict[str, Any] = {
        "processing.caseSectionsExtractedAt": utc_now(),
        "processing.caseSectionsError": None,
        "processing.caseSectionsMeta": meta,
        "audit.updatedAt": utc_now(),
        "audit.lastSectionsExtractedAt": utc_now(),
        "status.pipelineStatus": "caseSectionsExtracted",
    }
    update.update(sections)

    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(col: Collection, doc_id: Any, *, err: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "processing.caseSectionsExtractedAt": utc_now(),
            "processing.caseSectionsError": err,
            "audit.updatedAt": utc_now(),
        }},
    )


# =============================================================================
# 4) MAIN
# =============================================================================

def _prompt_mode() -> Tuple[str, Optional[str]]:
    """
    Pergunta ao usuario o modo de execucao.
    Retorna (mode, stf_decision_id).
    mode in {"all", "one"}
    """
    print("\nSelecione o modo de execucao:")
    print("  1) Processar TODOS com status.pipelineStatus = caseHtmlCleaned")
    print("  2) Processar APENAS um documento por stfDecisionId")

    while True:
        choice = input("Opcao (1/2): ").strip()
        if choice == "1":
            return "all", None
        if choice == "2":
            stf_decision_id = input("Informe o stfDecisionId: ").strip()
            if stf_decision_id:
                return "one", stf_decision_id
            print("stfDecisionId vazio. Tente novamente.")
        else:
            print("Opcao invalida. Use 1 ou 2.")


def _log_doc_header(doc: Dict[str, Any]) -> None:
    doc_id = doc.get("_id")
    stf_decision_id = (doc.get("identity") or {}).get("stfDecisionId")
    title = doc.get("caseTitle") or "N/A"
    status = (doc.get("status", {}) or {}).get("pipelineStatus")
    log("INFO", f"Documento | _id={doc_id} | stfDecisionId={stf_decision_id} | status={status} | title='{title}'")


def process_document(col: Collection, doc: Dict[str, Any]) -> bool:
    doc_id = doc.get("_id")

    try:
        col.update_one(
            {"_id": doc_id},
            {"$set": {"processing.caseSectionsExtractingAt": utc_now()}},
        )

        case_html_clean = ((doc.get("caseContent") or {}).get("caseHtmlClean") or "").strip()
        if not case_html_clean:
            raise ValueError("Campo caseContent.caseHtmlClean vazio.")

        log("STEP", "Extraindo secoes do HTML sanitizado")
        sections, meta = extract_sections(case_html_clean)

        if not sections:
            raise RuntimeError("Nenhuma secao localizada a partir de caseHtmlClean.")

        log("OK", f"Secoes encontradas: {', '.join(meta.get('found') or [])}")
        log("STEP", "Persistindo secoes + status")
        persist_success(col, doc_id, sections=sections, meta=meta)
        log("OK", "Documento atualizado com sucesso")
        return True

    except Exception as e:
        log("ERROR", f"Erro ao processar documento | _id={doc_id} | erro={e}")
        persist_error(col, doc_id, err=str(e))
        return False


def main() -> int:
    log("INFO", "Iniciando etapa: EXTRAIR SECOES DO PROCESSO (caseHtmlClean)")

    try:
        col = get_case_data_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    mode, stf_decision_id = _prompt_mode()

    if mode == "all":
        base_filter: Dict[str, Any] = {"status.pipelineStatus": "caseHtmlCleaned"}
        log("STEP", "Buscando documentos com status.pipelineStatus = caseHtmlCleaned")
        try:
            cursor = col.find(
                base_filter,
                projection={"caseContent.caseHtmlClean": 1, "identity.stfDecisionId": 1, "caseTitle": 1, "status.pipelineStatus": 1},
            )
        except PyMongoError as e:
            log("ERROR", f"Erro ao consultar documentos: {e}")
            return 1

        total = 0
        ok = 0
        for doc in cursor:
            total += 1
            _log_doc_header(doc)
            if process_document(col, doc):
                ok += 1

        log("INFO", f"Finalizado | total={total} | ok={ok} | erro={total - ok}")
        return 0 if total == ok else 1

    log("STEP", f"Buscando documento por identity.stfDecisionId='{stf_decision_id}'")
    try:
        doc = col.find_one(
            {"identity.stfDecisionId": stf_decision_id},
            projection={"caseContent.caseHtmlClean": 1, "identity.stfDecisionId": 1, "caseTitle": 1, "status.pipelineStatus": 1},
        )
    except PyMongoError as e:
        log("ERROR", f"Erro ao consultar documento: {e}")
        return 1

    if not doc:
        log("WARN", f"Nenhum documento encontrado para identity.stfDecisionId='{stf_decision_id}'")
        return 1

    _log_doc_header(doc)
    ok = process_document(col, doc)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
