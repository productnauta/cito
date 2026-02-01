#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
e_sanitize_case_html.py

Objetivo:
- Ler na collection "case_data" o documento mais antigo apto para sanitização:
    - caseContent.originalHtml existe e não está vazio
    - caseContent.cleanHtm não existe (ou vazio), a menos que FORCE_RESANITIZE=true
- Sanitizar o HTML removendo:
    - scripts, css, noscript, iframes, embeds, svg/canvas, forms etc.
    - atributos ruidosos (style, on*, data-*, aria-* etc.)
- Manter apenas marcação mínima útil para localizar informações:
    - títulos (h1..h6), parágrafos, listas, tabelas, links (a[href])
    - (opcional) id/class em alguns containers para facilitar seletores
- Persistir em: caseContent.cleanHtm
- Atualizar status/auditoria (se existir no documento)
- Exibir mensagens detalhadas no terminal

Dependências:
  pip install pymongo beautifulsoup4
"""

from __future__ import annotations

import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Set, Tuple

from bs4 import BeautifulSoup, Comment
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection


# =============================================================================
# 0) LOG
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


def _size_kb(text: str) -> int:
    if not text:
        return 0
    return int((len(text.encode("utf-8")) + 1023) / 1024)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# 1) CONFIG (JSON)
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
QUERY_CONFIG_PATH = CONFIG_DIR / "query.json"

CASE_DATA_COLLECTION = "case_data"

# Controle de execução (podem ficar em query.json -> pipeline.sanitize_html.*)
DEFAULT_FORCE_RESANITIZE = False
DEFAULT_CLAIM_INPUT_STATUS = None       # ex.: "caseScraped" | None = ignora filtro por status
DEFAULT_SET_OUTPUT_STATUS = None        # ex.: "caseSanitized" | None = não altera status
DEFAULT_DELAY_BETWEEN_ITEMS_SEC = 0.0   # ex.: 1.5


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


@dataclass(frozen=True)
class SanitizeCfg:
    force_resanitize: bool
    claim_input_status: Optional[str]
    set_output_status: Optional[str]
    delay_between_items_sec: float


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def _get(d: Dict[str, Any], path: str, default: Any = None) -> Any:
    """Getter por dotted-path (ex.: 'pipeline.sanitize_html.force_resanitize')."""
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict):
            return default
        if part not in cur:
            return default
        cur = cur[part]
    return cur


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    mongo = raw.get("mongo")
    if not isinstance(mongo, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida.")
    uri = str(mongo.get("uri") or "").strip()
    db = str(mongo.get("database") or "").strip()
    if not uri:
        raise ValueError("Config inválida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config inválida: 'mongo.database' vazio.")
    return MongoCfg(uri=uri, database=db)


def build_sanitize_cfg(query_raw: Dict[str, Any]) -> SanitizeCfg:
    """
    Lê parâmetros opcionais de execução em query.json (se existirem):

    {
      "pipeline": {
        "sanitize_html": {
          "force_resanitize": false,
          "claim_input_status": "caseHtmlFetched",
          "set_output_status": "caseSanitized",
          "delay_between_items_sec": 0.5
        }
      }
    }
    """
    force_resanitize = bool(_get(query_raw, "pipeline.sanitize_html.force_resanitize", DEFAULT_FORCE_RESANITIZE))
    claim_input_status = _get(query_raw, "pipeline.sanitize_html.claim_input_status", DEFAULT_CLAIM_INPUT_STATUS)
    set_output_status = _get(query_raw, "pipeline.sanitize_html.set_output_status", DEFAULT_SET_OUTPUT_STATUS)
    delay_between_items_sec = float(_get(query_raw, "pipeline.sanitize_html.delay_between_items_sec", DEFAULT_DELAY_BETWEEN_ITEMS_SEC))

    # normalizações leves
    claim_input_status = str(claim_input_status).strip() if isinstance(claim_input_status, str) and claim_input_status.strip() else None
    set_output_status = str(set_output_status).strip() if isinstance(set_output_status, str) and set_output_status.strip() else None
    delay_between_items_sec = max(0.0, delay_between_items_sec)

    return SanitizeCfg(
        force_resanitize=force_resanitize,
        claim_input_status=claim_input_status,
        set_output_status=set_output_status,
        delay_between_items_sec=delay_between_items_sec,
    )


def get_case_data_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("STEP", "Validando conexão (ping)")
    client.admin.command("ping")
    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")

    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) SANITIZAÇÃO (HTML -> cleanHtm)
# =============================================================================

# Marcadores típicos de páginas “challenge/bot” (evita gravar lixo)
_WAF_MARKERS = (
    "awswaf",
    "challenge.js",
    "verify that you're not a robot",
    "javascript is disabled",
    "challenge-container",
    "awswafintegration",
)

REMOVE_TAGS = {
    # recursos/metadados e execução
    "head", "meta", "title", "link", "base",
    "script", "style", "noscript",
    # embeds
    "iframe", "object", "embed", "param",
    # mídia
    "picture", "source", "video", "audio",
    # svg/canvas
    "svg", "canvas",
    # forms/inputs
    "form", "input", "button", "select", "option", "textarea", "label",
}

# Mantém estrutura mínima útil para extrações
ALLOWED_TAGS: Set[str] = {
    "div", "section", "article",
    "p", "br", "hr",
    "span",  # permitido mas será simplificado (unwrap) quando possível
    "h1", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li",
    "table", "thead", "tbody", "tr", "th", "td",
    "blockquote",
    "a",
    "b", "strong", "i", "em", "u",
}

# Permite poucos atributos para localização (id/class) + links (href)
ALLOWED_ATTRS: Dict[str, Set[str]] = {
    "a": {"href"},
    "div": {"id", "class"},
    "section": {"id", "class"},
    "article": {"id", "class"},
    "h1": {"id", "class"}, "h2": {"id", "class"}, "h3": {"id", "class"},
    "h4": {"id", "class"}, "h5": {"id", "class"}, "h6": {"id", "class"},
}

DROP_ATTR_PREFIXES = ("on", "data-", "aria-")
DROP_ATTRS = {"style", "src", "srcset", "sizes", "role", "tabindex", "integrity", "crossorigin", "referrerpolicy"}


def detect_waf_challenge(html: str) -> bool:
    low = (html or "").lower()
    return any(m in low for m in _WAF_MARKERS)


def _select_main_content(soup: BeautifulSoup) -> BeautifulSoup:
    """
    Tenta isolar o conteúdo principal do STF (reduz ruído).
    Se não encontrar, mantém o conteúdo inteiro já “limpo”.
    """
    main = soup.find("div", class_="mat-tab-body-wrapper")
    if main:
        return BeautifulSoup(str(main), "html.parser")
    return soup


def sanitize_html_for_extraction(html: str) -> Tuple[str, Dict[str, Any]]:
    """
    Sanitiza HTML para facilitar extrações posteriores.
    Retorna (clean_html, meta).
    """
    meta: Dict[str, Any] = {
        "wafChallenge": False,
        "removedTagsCount": 0,
        "unwrappedTagsCount": 0,
        "keptTagsCount": 0,
    }

    if not html or not html.strip():
        return "", meta

    if detect_waf_challenge(html):
        meta["wafChallenge"] = True
        return "", meta

    soup = BeautifulSoup(html, "html.parser")

    # Remove comentários HTML (reduz ruído)
    for c in soup.find_all(string=lambda t: isinstance(t, Comment)):
        c.extract()

    # Remove tags proibidas (agressivo)
    for tag in list(soup.find_all(True)):
        if tag.name in REMOVE_TAGS:
            meta["removedTagsCount"] += 1
            tag.decompose()

    # Isola o “miolo” quando disponível
    soup = _select_main_content(soup)

    # Segunda passada: remove lixo que possa ter sobrado dentro do main
    for tag in list(soup.find_all(True)):
        if tag.name in REMOVE_TAGS:
            meta["removedTagsCount"] += 1
            tag.decompose()

    # Normaliza tags/atributos
    for tag in list(soup.find_all(True)):
        if tag.name not in ALLOWED_TAGS:
            meta["unwrappedTagsCount"] += 1
            tag.unwrap()
            continue

        allowed = ALLOWED_ATTRS.get(tag.name, set())
        new_attrs: Dict[str, Any] = {}

        for k, v in (tag.attrs or {}).items():
            k_low = k.lower()

            if any(k_low.startswith(p) for p in DROP_ATTR_PREFIXES):
                continue
            if k_low in DROP_ATTRS:
                continue
            if k_low not in allowed:
                continue

            if tag.name == "a" and k_low == "href":
                href = str(v).strip()
                if href:
                    new_attrs["href"] = href
            elif k_low == "class" and isinstance(v, list):
                new_attrs["class"] = [str(x).strip() for x in v if str(x).strip()]
            else:
                new_attrs[k_low] = v

        tag.attrs = new_attrs

        # Simplifica spans sem atributos (reduz nós inúteis)
        if tag.name == "span" and not tag.attrs:
            meta["unwrappedTagsCount"] += 1
            tag.unwrap()

    # Remove blocos vazios
    for tag in list(soup.find_all(["div", "section", "article", "p", "li"])):
        txt = tag.get_text(" ", strip=True)
        if not txt and not tag.find("a"):
            tag.decompose()

    out_html = str(soup).strip()
    out_html = re.sub(r">\s+<", "><", out_html)  # compacta espaços entre tags

    meta["keptTagsCount"] = len(BeautifulSoup(out_html, "html.parser").find_all(True))
    return out_html, meta


# =============================================================================
# 3) CLAIM + UPDATE
# =============================================================================

def claim_next_case_for_sanitization(col: Collection, cfg: SanitizeCfg) -> Optional[Dict[str, Any]]:
    """
    Claim atômico do documento mais antigo apto para sanitização.
    """
    base_filter: Dict[str, Any] = {
        "caseContent.originalHtml": {"$exists": True, "$nin": [None, ""]},
    }

    # Filtro opcional por pipelineStatus (quando o seu schema tiver status.pipelineStatus)
    if cfg.claim_input_status:
        base_filter["status.pipelineStatus"] = cfg.claim_input_status

    # Default: só sanitiza se cleanHtm não existe/está vazio
    if not cfg.force_resanitize:
        base_filter["$or"] = [
            {"caseContent.cleanHtm": {"$exists": False}},
            {"caseContent.cleanHtm": None},
            {"caseContent.cleanHtm": ""},
        ]

    log("STEP", "Claim atômico do próximo documento apto para sanitização")
    return col.find_one_and_update(
        base_filter,
        {
            "$set": {
                "processing.cleanHtmlSanitizingAt": utc_now(),
                # não muda pipelineStatus aqui (evita conflito com outros stages)
            }
        },
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def persist_clean_html(col: Collection, doc_id, *, clean_html: str, cfg: SanitizeCfg, meta: Dict[str, Any]) -> None:
    """
    Persiste o HTML sanitizado e registra métricas.
    """
    update: Dict[str, Any] = {
        "caseContent.cleanHtm": clean_html,  # conforme solicitado
        "processing.cleanHtmlSanitizedAt": utc_now(),
        "processing.cleanHtmlMeta": meta,
        "processing.cleanHtmlError": None,
        "audit.updatedAt": utc_now(),
        "audit.lastExtractedAt": utc_now(),
    }

    # Atualiza pipelineStatus apenas se configurado
    if cfg.set_output_status:
        update["status.pipelineStatus"] = cfg.set_output_status

    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(col: Collection, doc_id, *, err: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "processing.cleanHtmlError": err,
            "processing.cleanHtmlSanitizedAt": utc_now(),
            "audit.updatedAt": utc_now(),
        }},
    )


# =============================================================================
# 4) MAIN (processa 1 item por execução)
# =============================================================================

def main() -> int:
    log("INFO", "Iniciando etapa: SANITIZAR HTML (case_data.caseContent.originalHtml -> caseContent.cleanHtm)")

    # Carrega configs
    log("STEP", f"Lendo config pipeline (opcional): {QUERY_CONFIG_PATH.resolve()}")
    query_raw: Dict[str, Any] = {}
    if QUERY_CONFIG_PATH.exists():
        query_raw = load_json(QUERY_CONFIG_PATH)
        log("OK", "query.json carregado")
    else:
        log("WARN", "query.json não encontrado; usando defaults")

    sanitize_cfg = build_sanitize_cfg(query_raw)
    log("INFO", f"force_resanitize={sanitize_cfg.force_resanitize} | "
                f"claim_input_status={sanitize_cfg.claim_input_status!r} | "
                f"set_output_status={sanitize_cfg.set_output_status!r} | "
                f"delay_between_items_sec={sanitize_cfg.delay_between_items_sec}")

    # Conecta no MongoDB e obtém collection
    col = get_case_data_collection()

    # Claim do próximo doc
    doc = claim_next_case_for_sanitization(col, sanitize_cfg)
    if not doc:
        log("INFO", "Nenhum documento apto encontrado. Finalizando.")
        return 0

    doc_id = doc["_id"]
    stf_id = (doc.get("identity", {}) or {}).get("stfDecisionId")
    title = (doc.get("stfCard", {}) or {}).get("caseTitle") or (doc.get("caseTitle") or "N/A")

    log("INFO", f"Documento selecionado | _id={doc_id} | stfDecisionId={stf_id} | title='{title}'")

    try:
        original_html = ((doc.get("caseContent", {}) or {}).get("originalHtml") or "").strip()
        if not original_html:
            raise ValueError("Campo caseContent.originalHtml vazio.")

        log("STEP", "Sanitizando HTML...")
        log("INFO", f"Tamanho original: {_size_kb(original_html)} KB")

        clean_html, meta = sanitize_html_for_extraction(original_html)

        if meta.get("wafChallenge"):
            raise RuntimeError("HTML retornado parece ser página de challenge/bot (AWS WAF).")

        if not clean_html.strip():
            raise RuntimeError("Sanitização resultou em HTML vazio (verifique seletores/conteúdo).")

        log("OK", "Sanitização concluída")
        log("INFO", f"Tamanho sanitizado: {_size_kb(clean_html)} KB")
        log("INFO", f"Métricas | removed={meta.get('removedTagsCount')} | unwrapped={meta.get('unwrappedTagsCount')} | kept={meta.get('keptTagsCount')}")

        log("STEP", "Persistindo HTML sanitizado em caseContent.cleanHtm")
        persist_clean_html(col, doc_id, clean_html=clean_html, cfg=sanitize_cfg, meta=meta)
        log("OK", "Documento atualizado com sucesso")

        # Delay opcional entre itens (quando você rodar em loop externo)
        if sanitize_cfg.delay_between_items_sec > 0:
            log("INFO", f"Aguardando delay: {sanitize_cfg.delay_between_items_sec:.2f}s")
            time.sleep(sanitize_cfg.delay_between_items_sec)

        log("INFO", f"Finalizado | _id={doc_id}")
        return 0

    except Exception as e:
        log("ERROR", f"Falha no processamento | _id={doc_id} | erro={e}")
        persist_error(col, doc_id, err=str(e))
        return 1


if __name__ == "__main__":
    sys.exit(main())
