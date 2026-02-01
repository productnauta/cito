#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
c_extract_data_case_data_v2_json.py

Objetivo:
- Ler configs de:
  - ./config/mongo.json
  - ./config/query.json (opcional nesta etapa; mantido para padronização do projeto)
- Fazer claim atômico do documento mais antigo na collection "raw_html" com status="new"
- Extrair os "cards" de resultado do STF (div.result-container)
- Persistir cada decisão na collection "case_data" no formato solicitado
- Somente criar campos quando houver valor útil (não inventar/evitar null desnecessário)
- Atualizar status do raw_html: new -> extracting -> extracted (ou error)

Dependências:
  pip install pymongo beautifulsoup4
"""

from __future__ import annotations

import json
import re
import sys
import traceback
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse, parse_qs

from bs4 import BeautifulSoup
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# =============================================================================
# 0) PATHS CONFIG
# =============================================================================

# Diretório base do script atual.
BASE_DIR = Path(__file__).resolve().parent

# Pasta padrão onde o usuário salvou os arquivos de configuração.
CONFIG_DIR = BASE_DIR / "config"

# Config do MongoDB (uri, database e opcionais: collections/statuses).
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"

# Config de query (não é obrigatório nesta etapa, mas pode ser útil para padronizar
# caminhos, logs ou compatibilidade com o pipeline).
QUERY_CONFIG_PATH = CONFIG_DIR / "query.json"


# =============================================================================
# 1) LOG / TIME
# =============================================================================

def utc_now() -> datetime:
    """Retorna timestamp UTC atual."""
    return datetime.now(timezone.utc)


def _ts() -> str:
    """Timestamp local para logs humanos no terminal."""
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    """Logger simples com timestamp."""
    print(f"[{_ts()}] [{level}] {msg}")


def step(n: int, total: int, msg: str) -> None:
    """Padroniza impressão de etapas."""
    log("STEP", f"({n}/{total}) {msg}")


# =============================================================================
# 2) CONFIG LOADER
# =============================================================================

def load_json(path: Path) -> Dict[str, Any]:
    """
    Lê e parseia um arquivo JSON.
    Erra com mensagem clara se o arquivo não existir.
    """
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


@dataclass(frozen=True)
class MongoCfg:
    """
    Configuração consolidada do MongoDB e do fluxo de status.

    - raw_html_collection / case_data_collection podem ser definidos no JSON
    - statuses podem ser definidos no JSON (para evitar hardcode)
    """
    uri: str
    database: str
    raw_html_collection: str
    case_data_collection: str
    raw_status_input: str
    raw_status_processing: str
    raw_status_ok: str
    raw_status_error: str


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    """
    Interpreta mongo.json.

    Estrutura suportada:

    {
      "mongo": {
        "uri": "...",
        "database": "...",
        "collections": {                # opcional
          "raw_html": "raw_html",
          "case_data": "case_data"
        },
        "raw_statuses": {               # opcional
          "input": "new",
          "processing": "extracting",
          "ok": "extracted",
          "error": "error"
        }
      }
    }
    """
    m = raw.get("mongo", {}) or {}

    uri = str(m.get("uri") or "").strip()
    database = str(m.get("database") or "").strip()
    if not uri or not database:
        raise ValueError("mongo.json inválido: campos obrigatórios 'mongo.uri' e 'mongo.database'")

    collections = m.get("collections", {}) if isinstance(m.get("collections"), dict) else {}
    raw_statuses = m.get("raw_statuses", {}) if isinstance(m.get("raw_statuses"), dict) else {}

    return MongoCfg(
        uri=uri,
        database=database,
        raw_html_collection=str(collections.get("raw_html") or "raw_html"),
        case_data_collection=str(collections.get("case_data") or "case_data"),
        raw_status_input=str(raw_statuses.get("input") or "new"),
        raw_status_processing=str(raw_statuses.get("processing") or "extracting"),
        raw_status_ok=str(raw_statuses.get("ok") or "extracted"),
        raw_status_error=str(raw_statuses.get("error") or "error"),
    )


# =============================================================================
# 3) UTILS (limpeza / setters condicionais)
# =============================================================================

def _clean_str(v: Any) -> Optional[str]:
    """
    Normaliza strings:
    - None -> None
    - '' / espaços -> None
    - 'N/A' -> None
    """
    if v is None:
        return None
    s = str(v).strip()
    if not s or s == "N/A":
        return None
    return s


def _clean_ws(s: str) -> str:
    """Compacta espaços em branco para facilitar logs/armazenamento."""
    return re.sub(r"\s+", " ", (s or "")).strip()


def _set_if(doc: Dict[str, Any], key: str, value: Any) -> None:
    """
    Cria doc[key] apenas se value for "útil":
    - str não vazia
    - dict/list não vazios
    - outros tipos: aceita se não for None
    """
    if value is None:
        return

    if isinstance(value, str):
        v = _clean_str(value)
        if v is None:
            return
        doc[key] = v
        return

    if isinstance(value, dict):
        if value:
            doc[key] = value
        return

    if isinstance(value, list):
        if value:
            doc[key] = value
        return

    doc[key] = value


def _subdoc_if_any(pairs: List[Tuple[str, Any]]) -> Optional[Dict[str, Any]]:
    """Cria um subdocumento somente se houver pelo menos um campo útil."""
    out: Dict[str, Any] = {}
    for k, v in pairs:
        _set_if(out, k, v)
    return out or None


# =============================================================================
# 4) MONGO HELPERS
# =============================================================================

def get_collections(cfg: MongoCfg) -> Tuple[Collection, Collection]:
    """
    Abre conexão MongoDB e retorna:
    - raw_html_collection
    - case_data_collection
    """
    log("INFO", "Conectando ao MongoDB...")
    client = MongoClient(cfg.uri)
    db = client[cfg.database]
    log("INFO", f"MongoDB conectado | db='{cfg.database}'")
    log("INFO", f"Collections | raw_html='{cfg.raw_html_collection}' | case_data='{cfg.case_data_collection}'")
    return db[cfg.raw_html_collection], db[cfg.case_data_collection]


def claim_next_raw_html(raw_col: Collection, cfg: MongoCfg) -> Optional[Dict[str, Any]]:
    """
    Claim atômico:
    - pega o raw_html mais antigo com status=input
    - marca como status=processing
    """
    log("INFO", f"Realizando claim atômico | status='{cfg.raw_status_input}' -> '{cfg.raw_status_processing}'")
    return raw_col.find_one_and_update(
        {"status": cfg.raw_status_input},
        {"$set": {"status": cfg.raw_status_processing, "extractingAt": utc_now()}},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )


def mark_raw_ok(raw_col: Collection, raw_id, cfg: MongoCfg, *, extracted_count: int) -> None:
    """Marca raw_html como OK e salva contagem extraída."""
    raw_col.update_one(
        {"_id": raw_id, "status": cfg.raw_status_processing},
        {"$set": {
            "status": cfg.raw_status_ok,
            "processedDate": utc_now(),
            "extractedCount": int(extracted_count),
        }},
    )


def mark_raw_error(raw_col: Collection, raw_id, cfg: MongoCfg, *, error_msg: str) -> None:
    """Marca raw_html como erro e salva mensagem sanitizada."""
    raw_col.update_one(
        {"_id": raw_id, "status": cfg.raw_status_processing},
        {"$set": {
            "status": cfg.raw_status_error,
            "processedDate": utc_now(),
            "error": _clean_ws(error_msg),
        }},
    )


# =============================================================================
# 5) EXTRAÇÃO DOS CARDS (result-container)
# =============================================================================

def _find_result_containers(soup: BeautifulSoup):
    """Localiza todos os cards de resultado."""
    return soup.find_all("div", class_="result-container")


def _extract_stf_decision_id(container) -> Optional[str]:
    """
    Extrai identificador do STF a partir do href do link principal do card.
    Heurística:
    - se achar parte iniciando com 'sjur' usa essa
    - senão usa o último segmento do path
    """
    link = container.find("a", class_="mat-tooltip-trigger")
    if link and link.has_attr("href"):
        href = link["href"]
        parts = [p for p in href.split("/") if p]
        for part in reversed(parts):
            if part.startswith("sjur"):
                return part
        if parts:
            return parts[-1]
    return None


def _extract_case_title(container) -> Optional[str]:
    """Extrai título do caso (texto do h4) do card."""
    h4 = container.find("h4", class_="ng-star-inserted")
    if h4:
        return _clean_str(h4.get_text(" ", strip=True))

    # fallback: h4 dentro do link
    link = container.find("a", class_="mat-tooltip-trigger")
    if link:
        h4_in = link.find("h4", class_="ng-star-inserted")
        if h4_in:
            return _clean_str(h4_in.get_text(" ", strip=True))
    return None


def _extract_case_url(container) -> Optional[str]:
    """Extrai URL do processo/decisão."""
    link = container.find("a", class_="mat-tooltip-trigger")
    if link and link.has_attr("href"):
        href = (link["href"] or "").strip()
        if not href:
            return None
        if href.startswith("http"):
            return href
        return f"https://jurisprudencia.stf.jus.br{href}"
    return None


def _extract_labeled_value(container, label_contains: str) -> Optional[str]:
    """
    Heurística: acha um elemento cujo texto contenha um label, e tenta pegar o valor
    próximo (span seguinte) ou trecho após ':'.
    """
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            nxt = el.find_next("span")
            if nxt:
                return _clean_str(nxt.get_text(" ", strip=True))
            if ":" in txt:
                return _clean_str(txt.split(":", 1)[1])
    return None


def _extract_date_by_regex(container, label_contains: str) -> Optional[str]:
    """
    Procura uma data no formato dd/mm/aaaa em elementos próximos de um label (Julgamento/Publicação).
    """
    for el in container.find_all(["h4", "span", "div"]):
        txt = el.get_text(" ", strip=True)
        if label_contains in txt:
            m = re.search(r"\d{2}/\d{2}/\d{4}", txt)
            if m:
                return m.group(0)
            nxt = el.find_next("span")
            if nxt:
                return _clean_str(nxt.get_text(" ", strip=True))
    return None


def _extract_case_class(container, fallback_title: Optional[str]) -> Optional[str]:
    """Extrai a classe a partir do href (classe=) ou por fallback (sigla inicial do título)."""
    for a in container.find_all("a"):
        if a.has_attr("href") and "classe=" in a["href"]:
            parsed = urlparse(a["href"])
            qs = parse_qs(parsed.query)
            if "classe" in qs and qs["classe"]:
                return _clean_str(qs["classe"][0])
    if fallback_title:
        parts = fallback_title.split()
        if parts and re.fullmatch(r"[A-Z]{2,}", parts[0]):
            return parts[0]
    return None


def _extract_case_number(container, fallback_title: Optional[str]) -> Optional[str]:
    """Extrai número do processo do href (numeroProcesso=) ou por fallback em regex no título."""
    for a in container.find_all("a"):
        if a.has_attr("href") and "numeroProcesso=" in a["href"]:
            parsed = urlparse(a["href"])
            qs = parse_qs(parsed.query)
            if "numeroProcesso" in qs and qs["numeroProcesso"]:
                return _clean_str(qs["numeroProcesso"][0])
    if fallback_title:
        nums = re.findall(r"\d[\d\.\-]*", fallback_title)
        if nums:
            return nums[-1]
    return None


def _extract_occurrences(container, keyword: str) -> Optional[int]:
    """
    Captura contagens do tipo:
      'Inteiro teor (12)'
      'Indexação (3)'
    """
    texts = container.find_all(string=re.compile(keyword, re.IGNORECASE))
    for t in texts:
        m = re.search(r"\((\d+)\)", str(t))
        if m:
            try:
                return int(m.group(1))
            except Exception:
                return None
    return None


def _extract_dom_id(node) -> Optional[str]:
    """Extrai o atributo id do container (se existir)."""
    if node and getattr(node, "attrs", None) and "id" in node.attrs:
        return _clean_str(node.attrs.get("id"))
    return None


def _derive_from_title(case_title: str) -> Dict[str, str]:
    """
    Deriva campos úteis do título:
    - caseCode: aqui o título inteiro
    - caseClassDetail: sigla inicial
    - caseNumberDetail: primeiro grupo numérico
    """
    out: Dict[str, str] = {}
    title = _clean_ws(case_title)
    if not title:
        return out

    out["caseCode"] = title

    m_class = re.match(r"^([A-Z]{2,})\b", title)
    if m_class:
        out["caseClassDetail"] = m_class.group(1)

    m_num = re.search(r"\b(\d[\d\.\-]*)\b", title)
    if m_num:
        out["caseNumberDetail"] = m_num.group(1)

    return out


def extract_cards(html_raw: str, source_raw_id: str) -> List[Dict[str, Any]]:
    """
    Converte o HTML em BeautifulSoup e extrai docs case_data no formato esperado.
    """
    soup = BeautifulSoup(html_raw, "html.parser")
    containers = _find_result_containers(soup)

    log("INFO", f"Cards encontrados (result-container): {len(containers)}")

    out_docs: List[Dict[str, Any]] = []
    for idx, container in enumerate(containers, start=1):
        case_title = _extract_case_title(container)
        stf_id = _extract_stf_decision_id(container)
        case_url = _extract_case_url(container)

        # Regra mínima: sem id, não persiste.
        if not stf_id:
            log("WARN", f"Card #{idx}: sem stfDecisionId (ignorado)")
            continue

        now = utc_now()
        doc: Dict[str, Any] = {}

        # -----------------------------
        # Top-level
        # -----------------------------
        _set_if(doc, "caseTitle", case_title)

        # -----------------------------
        # identity/
        # -----------------------------
        identity_pairs: List[Tuple[str, Any]] = [
            ("stfDecisionId", stf_id),
            ("rawHtmlId", source_raw_id),
        ]
        if case_title:
            derived = _derive_from_title(case_title)
            identity_pairs.append(("caseCode", derived.get("caseCode")))
            identity_pairs.append(("caseClassDetail", derived.get("caseClassDetail")))
            identity_pairs.append(("caseNumberDetail", derived.get("caseNumberDetail")))
        _set_if(doc, "identity", _subdoc_if_any(identity_pairs))

        # -----------------------------
        # dates/
        # -----------------------------
        judgment_date = _extract_date_by_regex(container, "Julgamento")
        publication_date = _extract_date_by_regex(container, "Publicação")
        _set_if(doc, "dates", _subdoc_if_any([
            ("judgmentDate", judgment_date),
            ("publicationDate", publication_date),
        ]))

        # -----------------------------
        # caseContent/
        # -----------------------------
        _set_if(doc, "caseContent", _subdoc_if_any([
            ("caseUrl", case_url),
        ]))

        # -----------------------------
        # stfCard/
        # -----------------------------
        judging_body = _extract_labeled_value(container, "Órgão julgador")
        rapporteur = _extract_labeled_value(container, "Relator")
        opinion_writer = _extract_labeled_value(container, "Redator")

        case_class = _extract_case_class(container, case_title)
        case_number = _extract_case_number(container, case_title)

        full_text_occ = _extract_occurrences(container, "Inteiro teor")
        indexing_occ = _extract_occurrences(container, "Indexação")

        dom_result_id = _extract_dom_id(container)

        stf_card: Dict[str, Any] = {}
        _set_if(stf_card, "localIndex", idx)
        _set_if(stf_card, "caseTitle", case_title)
        _set_if(stf_card, "caseUrl", case_url)
        _set_if(stf_card, "caseClass", case_class)
        _set_if(stf_card, "caseNumber", case_number)
        _set_if(stf_card, "judgingBody", judging_body)
        _set_if(stf_card, "rapporteur", rapporteur)
        _set_if(stf_card, "opinionWriter", opinion_writer)
        _set_if(stf_card, "judgmentDate", judgment_date)
        _set_if(stf_card, "publicationDate", publication_date)

        # occurrences só se > 0
        occ_sub: Dict[str, Any] = {}
        if isinstance(full_text_occ, int) and full_text_occ > 0:
            occ_sub["fullText"] = full_text_occ
        if isinstance(indexing_occ, int) and indexing_occ > 0:
            occ_sub["indexing"] = indexing_occ
        _set_if(stf_card, "occurrences", occ_sub or None)

        _set_if(stf_card, "domResultContainerId", dom_result_id)

        # tenta capturar id de botão de clipboard (heurística)
        dom_clip = None
        for b in container.find_all("button"):
            if b.has_attr("id") and b.has_attr("mattooltip"):
                tip = (b.get("mattooltip") or "").lower()
                if any(w in tip for w in ("copiar", "copy", "link")):
                    dom_clip = _clean_str(b["id"])
                    break
        _set_if(stf_card, "domClipboardId", dom_clip)

        _set_if(doc, "stfCard", stf_card or None)

        # -----------------------------
        # audit/
        # -----------------------------
        audit: Dict[str, Any] = {}
        _set_if(audit, "extractionDate", now)
        _set_if(audit, "lastExtractedAt", now)
        _set_if(audit, "builtAt", now)
        _set_if(audit, "updatedAt", now)
        _set_if(audit, "sourceStatus", "extracted")
        _set_if(audit, "pipelineStatus", "extracted")
        _set_if(doc, "audit", audit)

        out_docs.append(doc)

    log("INFO", f"Docs gerados (case_data): {len(out_docs)}")
    return out_docs


def build_query_from_raw(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Monta subdoc query/ a partir do raw_html.
    Suporta:
      - formato novo: raw_doc.search.{queryString,pageSize,inteiroTeor}
      - formato legado: raw_doc.{queryString,pageSize,inteiroTeor}
    """
    search = raw_doc.get("search") if isinstance(raw_doc.get("search"), dict) else {}
    q_old = raw_doc

    query_string = _clean_str(search.get("queryString")) or _clean_str(q_old.get("queryString"))
    page_size = search.get("pageSize")
    inteiro_teor = search.get("inteiroTeor")

    # pageSize legado costuma ser string
    if page_size is None:
        ps = q_old.get("pageSize")
        if ps is not None:
            try:
                page_size = int(float(str(ps).strip().replace(",", ".")))
            except Exception:
                page_size = None

    # inteiroTeor legado pode ser bool ou string
    if inteiro_teor is None and "inteiroTeor" in q_old:
        it = q_old.get("inteiroTeor")
        if isinstance(it, bool):
            inteiro_teor = it
        else:
            s = str(it).strip().lower()
            if s in ("true", "1", "yes", "y", "on", "sim", "s"):
                inteiro_teor = True
            elif s in ("false", "0", "no", "n", "off", "nao", "não"):
                inteiro_teor = False
            else:
                inteiro_teor = None

    return _subdoc_if_any([
        ("queryString", query_string),
        ("pageSize", page_size),
        ("inteiroTeor", inteiro_teor),
    ]) or {}


def upsert_case_data(case_col: Collection, *, doc: Dict[str, Any], stf_decision_id: str) -> None:
    """
    UPSERT por identity.stfDecisionId.
    - Atualiza doc completo
    - Garante audit.updatedAt e audit.lastExtractedAt
    """
    now = utc_now()

    audit = doc.get("audit") if isinstance(doc.get("audit"), dict) else {}
    audit["updatedAt"] = now
    audit["lastExtractedAt"] = now
    doc["audit"] = audit

    set_on_insert: Dict[str, Any] = {}
    if "builtAt" not in audit:
        set_on_insert["audit.builtAt"] = now

    case_col.update_one(
        {"identity.stfDecisionId": stf_decision_id},
        {"$set": doc, "$setOnInsert": set_on_insert},
        upsert=True,
    )


# =============================================================================
# 6) MAIN (processa 1 raw_html por execução)
# =============================================================================

def main() -> int:
    total_steps = 8

    step(1, total_steps, "Carregando configurações (mongo.json / query.json)")
    mongo_raw = load_json(MONGO_CONFIG_PATH)
    mongo_cfg = build_mongo_cfg(mongo_raw)
    log("INFO", f"mongo.json OK | db='{mongo_cfg.database}'")

    # query.json não é obrigatório nesta etapa; loga apenas.
    try:
        _ = load_json(QUERY_CONFIG_PATH)
        log("INFO", f"query.json encontrado | path='{QUERY_CONFIG_PATH.resolve()}'")
    except FileNotFoundError:
        log("WARN", f"query.json não encontrado em {QUERY_CONFIG_PATH.resolve()} (ok para esta etapa)")

    step(2, total_steps, "Conectando ao MongoDB e obtendo collections")
    raw_col, case_col = get_collections(mongo_cfg)

    step(3, total_steps, f"Claim do próximo raw_html (status='{mongo_cfg.raw_status_input}')")
    raw_doc = claim_next_raw_html(raw_col, mongo_cfg)
    if not raw_doc:
        log("INFO", f"Nenhum documento com status='{mongo_cfg.raw_status_input}' em '{mongo_cfg.raw_html_collection}'.")
        return 0

    raw_id = raw_doc["_id"]
    raw_id_str = str(raw_id)
    log("INFO", f"Documento claimed | raw_html._id={raw_id_str} | status='{mongo_cfg.raw_status_processing}'")

    try:
        step(4, total_steps, "Lendo HTML do raw_html (formato novo e legado)")
        html_raw = (raw_doc.get("payload", {}).get("htmlRaw") if isinstance(raw_doc.get("payload"), dict) else None)
        if not html_raw:
            html_raw = raw_doc.get("htmlRaw")  # legado

        html_raw = (html_raw or "").strip()
        if not html_raw:
            raise ValueError("Documento raw_html não possui HTML (payload.htmlRaw/htmlRaw vazio).")

        log("INFO", f"HTML carregado | chars={len(html_raw)}")

        step(5, total_steps, "Extraindo dados de query do raw_html (injetar em case_data.query)")
        query_sub = build_query_from_raw(raw_doc)
        if query_sub:
            log("INFO", f"Query detectada | {query_sub}")
        else:
            log("WARN", "Query não detectada no raw_html (campo query não será incluído)")

        step(6, total_steps, "Extraindo cards do HTML")
        extracted_docs = extract_cards(html_raw, raw_id_str)

        # injeta query/ em cada doc se existir
        if query_sub:
            for d in extracted_docs:
                _set_if(d, "query", query_sub)

        step(7, total_steps, "Persistindo decisões (UPSERT em case_data)")
        saved = 0
        skipped = 0
        for i, d in enumerate(extracted_docs, start=1):
            identity = d.get("identity") if isinstance(d.get("identity"), dict) else {}
            stf_id = _clean_str(identity.get("stfDecisionId"))
            if not stf_id:
                skipped += 1
                log("WARN", f"Persistência: doc #{i} sem stfDecisionId (ignorado)")
                continue

            upsert_case_data(case_col, doc=d, stf_decision_id=stf_id)
            saved += 1

        log("INFO", f"Persistência concluída | salvos={saved} | ignorados={skipped}")

        step(8, total_steps, "Atualizando status do raw_html para 'extracted'")
        mark_raw_ok(raw_col, raw_id, mongo_cfg, extracted_count=len(extracted_docs))

        log("INFO", "Execução concluída com sucesso")
        log("INFO", f"raw_html._id={raw_id_str} | extractedCount={len(extracted_docs)} | status='{mongo_cfg.raw_status_ok}'")
        return 0

    except Exception as e:
        # Marca erro no raw_html e imprime stacktrace detalhado para diagnóstico.
        step(8, total_steps, "Falha detectada: marcando raw_html como 'error'")
        mark_raw_error(raw_col, raw_id, mongo_cfg, error_msg=str(e))

        log("ERROR", f"Erro ao processar raw_html._id={raw_id_str}: {e}")
        log("ERROR", "Stacktrace completo:")
        print(traceback.format_exc())

        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except PyMongoError as e:
        log("ERROR", f"Erro MongoDB: {e}")
        sys.exit(2)
