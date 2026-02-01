#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step04-extract-sessions.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts case sections from clean HTML and generates raw HTML and Markdown per section.
Inputs: config/mongo.json, case_data.caseContent.caseHtmlClean.
Outputs: caseContent.raw.* and caseContent.md.* fields; processing/status updates.
Pipeline: parse clean HTML -> sanitize fragments -> convert to Markdown -> persist sections.
Dependencies: pymongo beautifulsoup4 markdownify
------------------------------------------------------------

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
from markdownify import markdownify as md
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
# 2) NORMALIZACAO / SANITIZACAO / MARKDOWN
# =============================================================================

# Seções em "slug" (destino caseContent.raw.<slug> e caseContent.md.<slug>)
SECTION_SLUGS = {
    "Publicacao": "publication",
    "Partes": "parties",
    "Ementa": "summary",
    "Decisao": "decision",
    "Indexacao": "keywords",
    "Legislacao": "legislation",
    "Observacao": "notes",
    "Doutrina": "doctrine",
}

HEADER_SLUG = "header"


def _norm_space(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _norm_title(s: str) -> str:
    # Remocao simples de acentos comuns apenas para comparacao basica
    s = _norm_space(s)
    s = s.replace("\u00e7", "c").replace("\u00e3", "a").replace("\u00e1", "a").replace("\u00e9", "e")
    s = s.replace("\u00ed", "i").replace("\u00f3", "o").replace("\u00fa", "u")
    return s


def sanitize_html_fragment(fragment_html: str) -> str:
    """
    Sanitiza fragmento HTML sem destruir estrutura útil para markdownify.
    - Remove tags inseguras/ruído: script/style/noscript/iframe/object/embed
    - Normaliza <br> -> \\n
    - Remove atributos ruidosos: style e on*
    - Mantém headings, links, imagens, listas, tabelas (se existirem), etc.
    """
    if not fragment_html or not fragment_html.strip():
        return ""

    soup = BeautifulSoup(fragment_html, "html.parser")

    for tag in soup(["script", "style", "noscript", "iframe", "object", "embed"]):
        tag.decompose()

    for br in soup.find_all("br"):
        br.replace_with("\n")

    for t in soup.find_all(True):
        attrs = dict(t.attrs) if t.attrs else {}
        for k in list(attrs.keys()):
            lk = k.lower()
            if lk.startswith("on"):
                del t.attrs[k]
        if "style" in t.attrs:
            del t.attrs["style"]

    sanitized = str(soup)
    sanitized = re.sub(r"[ \t]+\n", "\n", sanitized)
    sanitized = re.sub(r"\n{3,}", "\n\n", sanitized)
    return sanitized.strip()


def normalize_markdown(md_text: str) -> str:
    """
    Normaliza Markdown:
    - remove trailing spaces
    - colapsa múltiplas linhas em branco
    - colapsa múltiplos espaços dentro da linha
    Preserva estrutura (headings/listas/links/imagens) ao NÃO achatar parágrafos.
    """
    if not md_text or not md_text.strip():
        return ""

    md_text = md_text.replace("\r", "\n")
    md_text = re.sub(r"[ \t]+\n", "\n", md_text)    # trailing spaces
    md_text = re.sub(r"\n{3,}", "\n\n", md_text)    # linhas em branco excessivas

    lines = []
    for ln in md_text.split("\n"):
        ln = re.sub(r"[ \t]{2,}", " ", ln).rstrip()
        lines.append(ln)

    out = "\n".join(lines).strip()
    out = re.sub(r"\n{3,}", "\n\n", out).strip()
    return out


def html_to_markdown(fragment_html: str) -> str:
    """
    HTML -> Markdown preservando headings/links/imagens/listas.
    """
    if not fragment_html or not fragment_html.strip():
        return ""

    md_text = md(
        fragment_html,
        heading_style="ATX",
        bullets="-",
        strip=["span"],
        escape_asterisks=False,
        escape_underscores=False,
    )
    return normalize_markdown(md_text)


def build_payload(fragment_html: str) -> Dict[str, str]:
    """
    Retorna:
      - raw_html: html sanitizado
      - md_text: markdown normalizado
    """
    raw_html = sanitize_html_fragment(fragment_html)
    md_text = html_to_markdown(raw_html)
    return {"raw_html": raw_html, "md_text": md_text}


# =============================================================================
# 3) EXTRACAO DE SECOES
# =============================================================================

def extract_sections(case_html_clean: str) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    Extrai secoes via padrão: div.jud-text > h4 + div.

    Retorna:
      - update_fields: dict pronto para $set (caseContent.raw.<slug>, caseContent.md.<slug>)
      - meta: info de encontrados/ausentes + tamanhos
    """
    update_fields: Dict[str, Any] = {}
    meta: Dict[str, Any] = {
        "found": [],
        "missing": [],
        "sizes": {},
    }

    if not case_html_clean or not case_html_clean.strip():
        return update_fields, meta

    soup = BeautifulSoup(case_html_clean, "html.parser")

    # 3.1) Seções por título (h4)
    for block in soup.find_all("div", class_="jud-text"):
        h4 = block.find("h4")
        if not h4:
            continue

        title = _norm_space(h4.get_text(" ", strip=True))
        if not title:
            continue

        norm_title = _norm_title(title)

        for desired_title, slug in SECTION_SLUGS.items():
            if _norm_title(desired_title) == norm_title:
                content_div = h4.find_next_sibling("div")
                html_fragment = content_div.decode_contents().strip() if content_div else ""

                payload = build_payload(html_fragment)

                raw_path = f"caseContent.raw.{slug}"
                md_path = f"caseContent.md.{slug}"

                update_fields[raw_path] = payload["raw_html"]
                update_fields[md_path] = payload["md_text"]

                break

    # 3.2) Header (identificacao principal) - heurística: primeiro jud-text cujo h4 contenha "ADI"
    header_html_fragment = ""
    for block in soup.find_all("div", class_="jud-text"):
        h4 = block.find("h4")
        if not h4:
            continue
        h4_text = _norm_space(h4.get_text(" ", strip=True))
        if "ADI" in h4_text:
            header_html_fragment = block.decode_contents().strip()
            break

    if header_html_fragment:
        payload = build_payload(header_html_fragment)
        update_fields[f"caseContent.raw.{HEADER_SLUG}"] = payload["raw_html"]
        update_fields[f"caseContent.md.{HEADER_SLUG}"] = payload["md_text"]

    # 3.3) Meta
    def _has(path: str) -> bool:
        return path in update_fields and update_fields.get(path) not in (None, "")

    # Seções previstas
    for _, slug in SECTION_SLUGS.items():
        raw_path = f"caseContent.raw.{slug}"
        md_path = f"caseContent.md.{slug}"
        if raw_path in update_fields or md_path in update_fields:
            meta["found"].append(slug)
        else:
            meta["missing"].append(slug)

        meta["sizes"][slug] = {
            "raw_len": len(update_fields.get(raw_path) or ""),
            "md_len": len(update_fields.get(md_path) or ""),
            "raw_ok": _has(raw_path),
            "md_ok": _has(md_path),
        }

    # Header
    h_slug = HEADER_SLUG
    meta["sizes"][h_slug] = {
        "raw_len": len(update_fields.get(f"caseContent.raw.{h_slug}") or ""),
        "md_len": len(update_fields.get(f"caseContent.md.{h_slug}") or ""),
        "raw_ok": _has(f"caseContent.raw.{h_slug}"),
        "md_ok": _has(f"caseContent.md.{h_slug}"),
    }
    if (f"caseContent.raw.{h_slug}" in update_fields) or (f"caseContent.md.{h_slug}" in update_fields):
        meta["found"].append(h_slug)
    else:
        meta["missing"].append(h_slug)

    return update_fields, meta


# =============================================================================
# 4) UPDATE HELPERS
# =============================================================================

def persist_success(col: Collection, doc_id: Any, *, update_fields: Dict[str, Any], meta: Dict[str, Any]) -> None:
    update: Dict[str, Any] = {
        "processing.caseSectionsExtractedAt": utc_now(),
        "processing.caseSectionsError": None,
        "processing.caseSectionsMeta": meta,
        "processing.pipelineStatus": "caseSectionsExtracted",
        "audit.updatedAt": utc_now(),
        "audit.lastSectionsExtractedAt": utc_now(),
        "status.pipelineStatus": "caseSectionsExtracted",
    }
    update.update(update_fields)
    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(col: Collection, doc_id: Any, *, err: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "processing.caseSectionsExtractedAt": utc_now(),
            "processing.caseSectionsError": err,
            "processing.pipelineStatus": "caseSectionsExtractError",
            "audit.updatedAt": utc_now(),
        }},
    )


# =============================================================================
# 5) MAIN
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

        log("STEP", "Extraindo secoes + sanitizando + convertendo para Markdown")
        update_fields, meta = extract_sections(case_html_clean)

        if not update_fields:
            raise RuntimeError("Nenhuma secao localizada a partir de caseHtmlClean.")

        found = meta.get("found") or []
        missing = meta.get("missing") or []
        log("OK", f"Encontradas: {', '.join(found) if found else 'nenhuma'}")
        if missing:
            log("WARN", f"Ausentes: {', '.join(missing)}")

        # Log tamanhos por secao
        sizes = meta.get("sizes") or {}
        for slug, info in sizes.items():
            log("INFO", f"  - {slug}: raw_len={info.get('raw_len')} | md_len={info.get('md_len')}")

        log("STEP", "Persistindo em caseContent.raw.<secao> e caseContent.md.<secao> + status")
        persist_success(col, doc_id, update_fields=update_fields, meta=meta)
        log("OK", "Documento atualizado com sucesso")
        return True

    except Exception as e:
        log("ERROR", f"Erro ao processar documento | _id={doc_id} | erro={e}")
        persist_error(col, doc_id, err=str(e))
        return False


def main() -> int:
    log("INFO", "Iniciando etapa: EXTRAIR SECOES DO PROCESSO (caseHtmlClean) -> raw/md")

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
                projection={
                    "caseContent.caseHtmlClean": 1,
                    "identity.stfDecisionId": 1,
                    "caseTitle": 1,
                    "status.pipelineStatus": 1,
                },
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
            projection={
                "caseContent.caseHtmlClean": 1,
                "identity.stfDecisionId": 1,
                "caseTitle": 1,
                "status.pipelineStatus": 1,
            },
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
