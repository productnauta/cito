#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set, Tuple

from bs4 import BeautifulSoup
from bson import ObjectId
from pymongo import MongoClient, ReturnDocument
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# ------------------------------------------------------------
# 0) MONGO CONFIG (mesmas credenciais/infos do projeto)
# ------------------------------------------------------------
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"

SOURCE_COLLECTION = "raw_html"   # origem
DEST_COLLECTION = "case_data"    # destino


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def iso_now() -> str:
    return utc_now().isoformat()


def safe_int(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        s = str(v).strip()
        if not s:
            return default
        return int(float(s.replace(",", ".")))
    except Exception:
        return default


def get_db_client() -> MongoClient:
    return MongoClient(MONGO_URI)


def get_collections(client: MongoClient) -> Tuple[Collection, Collection]:
    db = client[DB_NAME]
    return db[SOURCE_COLLECTION], db[DEST_COLLECTION]


def extract_html_from_raw_doc(raw_doc: Dict[str, Any]) -> str:
    """
    Suporta os dois formatos:
    - antigo: raw_doc["htmlRaw"]
    - novo:  raw_doc["payload"]["htmlRaw"]
    """
    if isinstance(raw_doc.get("htmlRaw"), str) and raw_doc.get("htmlRaw"):
        return raw_doc["htmlRaw"]
    payload = raw_doc.get("payload")
    if isinstance(payload, dict) and isinstance(payload.get("htmlRaw"), str):
        return payload["htmlRaw"]
    return ""


def str_objectid(v: Any) -> str:
    try:
        return str(v)
    except Exception:
        return ""


def _clean_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    if not s or s == "N/A":
        return None
    return s


def _clean_ws(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _set_if(doc: Dict[str, Any], key: str, value: Any) -> None:
    """
    Só cria o campo se houver valor útil.
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
    out: Dict[str, Any] = {}
    for k, v in pairs:
        _set_if(out, k, v)
    return out or None


def derive_case_class_detail(case_title: str) -> Optional[str]:
    s = _clean_str(case_title)
    if not s:
        return None
    parts = s.split()
    # sigla inicial (ADI/ADC/ADPF etc.)
    if parts and re.fullmatch(r"[A-Z]{2,}", parts[0]):
        return parts[0]
    return parts[0] if parts else None


def derive_case_number_detail(case_title: str) -> Optional[str]:
    s = _clean_str(case_title)
    if not s:
        return None
    m = re.search(r"\b(\d[\d\.\-]*)\b", s)
    return m.group(1) if m else None


def parse_date_ddmmyyyy(text: str) -> Optional[str]:
    if not text:
        return None
    m = re.search(r"\d{2}/\d{2}/\d{4}", text)
    return m.group(0) if m else None


def build_query_from_raw(raw_doc: Dict[str, Any]) -> Dict[str, Any]:
    """
    Retorna subdoc query/ a partir do raw_html (suporta dois formatos).
    Só inclui chaves com valor disponível.
    """
    search = raw_doc.get("search") if isinstance(raw_doc.get("search"), dict) else {}
    # compat antigo
    q_old = raw_doc

    query_string = _clean_str(search.get("queryString")) or _clean_str(q_old.get("queryString"))

    page_size = search.get("pageSize")
    if page_size is None:
        page_size = safe_int(q_old.get("pageSize"), 0) or None

    inteiro_teor = search.get("inteiroTeor")
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

    query_sub = _subdoc_if_any(
        [
            ("queryString", query_string),
            ("pageSize", page_size),
            ("inteiroTeor", inteiro_teor),
        ]
    )
    return query_sub or {}


# ------------------------------------------------------------
# Extração (parse dos result-container)
# ------------------------------------------------------------
class STFListExtractor:
    def extract_decisions(self, html: str, source_raw_id: ObjectId) -> List[Dict[str, Any]]:
        soup = BeautifulSoup(html, "html.parser")
        containers = soup.find_all("div", class_="result-container")

        decisions: List[Dict[str, Any]] = []
        for idx, container in enumerate(containers, start=1):
            data = self._extract_container_data(container, idx, source_raw_id)
            if data:
                decisions.append(data)
        return decisions

    def _extract_container_data(
        self,
        container: Any,
        local_index: int,
        source_raw_id: ObjectId,
    ) -> Optional[Dict[str, Any]]:
        try:
            stf_decision_id = self._extract_stf_decision_id(container)
            case_title = self._extract_case_title(container)
            case_url = self._extract_case_url(container)

            # regra: sem stfDecisionId não persiste
            if not stf_decision_id or stf_decision_id == "N/A":
                return None

            judging_body = self._extract_label_value(container, "Órgão julgador")
            rapporteur = self._extract_label_value(container, "Relator")
            opinion_writer = self._extract_label_value(container, "Redator")

            judgment_date = self._extract_date_by_label(container, "Julgamento")
            publication_date = self._extract_date_by_label(container, "Publicação")

            case_class = self._extract_case_class(container, case_title)
            case_number = self._extract_case_number(container, case_title)

            full_text_occ = self._extract_occurrences(container, "Inteiro teor")
            indexing_occ = self._extract_occurrences(container, "Indexação")

            dom_result_container_id = container.get("id") if hasattr(container, "get") else None
            dom_clipboard_id = self._extract_dom_clipboard_id(container)

            # Observação: mantém chaves planas aqui; a estrutura final é montada no builder
            return {
                "localIndex": local_index,
                "stfDecisionId": stf_decision_id,
                "caseTitle": case_title,
                "caseUrl": case_url,
                "judgingBody": judging_body,
                "rapporteur": rapporteur,
                "opinionWriter": opinion_writer,
                "judgmentDate": judgment_date,
                "publicationDate": publication_date,
                "caseClass": case_class,
                "caseNumber": case_number,
                "fullTextOccurrences": full_text_occ,
                "indexingOccurrences": indexing_occ,
                "domResultContainerId": dom_result_container_id,
                "domClipboardId": dom_clipboard_id,
                "sourceRawHtmlId": source_raw_id,                 # ObjectId (rastreamento interno)
                "sourceDocumentId": str_objectid(source_raw_id),  # compat (string)
            }
        except Exception:
            return None

    def _extract_stf_decision_id(self, container: Any) -> str:
        link = container.find("a", class_="mat-tooltip-trigger")
        if link and link.has_attr("href"):
            href = link["href"]
            parts = [p for p in href.split("/") if p]
            for part in reversed(parts):
                if part.startswith("sjur"):
                    return part
            if parts:
                return parts[-1]
        return "N/A"

    def _extract_case_title(self, container: Any) -> str:
        h4 = container.find("h4", class_="ng-star-inserted")
        if h4:
            return h4.get_text(" ", strip=True)
        link = container.find("a", class_="mat-tooltip-trigger")
        if link:
            h4_in = link.find("h4", class_="ng-star-inserted")
            if h4_in:
                return h4_in.get_text(" ", strip=True)
        return "N/A"

    def _extract_case_url(self, container: Any) -> str:
        link = container.find("a", class_="mat-tooltip-trigger")
        if link and link.has_attr("href"):
            href = (link["href"] or "").strip()
            if not href:
                return "N/A"
            if href.startswith("http"):
                return href
            return f"https://jurisprudencia.stf.jus.br{href}"
        return "N/A"

    def _extract_label_value(self, container: Any, label: str) -> str:
        elements = container.find_all(["h4", "span", "div"])
        for el in elements:
            txt = el.get_text(" ", strip=True)
            if label in txt:
                nxt = el.find_next("span")
                if nxt:
                    return nxt.get_text(" ", strip=True)
                parts = txt.split(":")
                if len(parts) > 1:
                    return parts[1].strip()
        return "N/A"

    def _extract_date_by_label(self, container: Any, label: str) -> str:
        elements = container.find_all(["h4", "span", "div"])
        for el in elements:
            txt = el.get_text(" ", strip=True)
            if label in txt:
                nxt = el.find_next("span")
                if nxt:
                    d = parse_date_ddmmyyyy(nxt.get_text(" ", strip=True))
                    if d:
                        return d
                d = parse_date_ddmmyyyy(txt)
                if d:
                    return d
        return "N/A"

    def _extract_case_class(self, container: Any, case_title: str) -> str:
        links = container.find_all("a")
        for link in links:
            href = link.get("href")
            if href and "classe=" in href:
                m = re.search(r"classe=([^&]+)", href)
                if m:
                    return m.group(1)
        d = derive_case_class_detail(case_title)
        return d or "N/A"

    def _extract_case_number(self, container: Any, case_title: str) -> str:
        links = container.find_all("a")
        for link in links:
            href = link.get("href")
            if href and "numeroProcesso=" in href:
                m = re.search(r"numeroProcesso=([^&]+)", href)
                if m:
                    return m.group(1)
        d = derive_case_number_detail(case_title)
        return d or "N/A"

    def _extract_occurrences(self, container: Any, label: str) -> int:
        texts = container.find_all(string=re.compile(label, re.IGNORECASE))
        for t in texts:
            parent = getattr(t, "parent", None)
            if parent is not None:
                txt = parent.get_text(" ", strip=True)
                m = re.search(r"\((\d+)\)", txt)
                if m:
                    try:
                        return int(m.group(1))
                    except Exception:
                        return 0
        return 0

    def _extract_dom_clipboard_id(self, container: Any) -> Optional[str]:
        buttons = container.find_all("button")
        for b in buttons:
            tip = b.get("mattooltip", "")
            if isinstance(tip, str) and any(w in tip.lower() for w in ["copiar", "copy", "link"]):
                if b.get("id"):
                    return b["id"]
        return None


# ------------------------------------------------------------
# Estrutura destino (case_data) - AJUSTADA para o documento atualizado
# ------------------------------------------------------------
def build_case_data_document(
    *,
    extracted: Dict[str, Any],
    raw_doc: Dict[str, Any],
    pipeline_status: str,
) -> Dict[str, Any]:
    """
    Gera documento compatível com a estrutura atualizada:

    - caseTitle (top-level)
    - identity/{caseCode,caseClassDetail,caseNumberDetail,caseDecisionType,stfDecisionId,rawHtmlId}
    - dates/{judgmentDate,publicationDate}
    - query/{queryString,pageSize,inteiroTeor}
    - caseContent/{rawHtml,sanitizedHtml,caseUrl}
    - stfCard/{... occurrences/{fullText,indexing} ...}
    - audit/{extractionDate,lastExtractedAt,builtAt,updatedAt,sourceStatus,pipelineStatus}

    Regra: se o dado não existe (None/"N/A"/vazio/0 para ocorrências), o campo não é criado.
    """
    now = utc_now()

    raw_id = raw_doc.get("_id")
    raw_id_str = str_objectid(raw_id)

    # ---- base values (extraídos)
    stf_id = _clean_str(extracted.get("stfDecisionId"))
    case_title = _clean_str(extracted.get("caseTitle"))
    case_url = _clean_str(extracted.get("caseUrl"))

    case_class_detail = derive_case_class_detail(case_title or "") if case_title else None
    case_number_detail = derive_case_number_detail(case_title or "") if case_title else None
    case_code = case_title  # conforme definido: "código derivado do título"

    judgment_date = _clean_str(extracted.get("judgmentDate"))
    publication_date = _clean_str(extracted.get("publicationDate"))

    # query/ vem do raw_doc
    query_sub = build_query_from_raw(raw_doc)

    # occurrences (só cria se > 0)
    full_text_occ = extracted.get("fullTextOccurrences")
    indexing_occ = extracted.get("indexingOccurrences")
    occ_sub: Dict[str, Any] = {}
    if isinstance(full_text_occ, int) and full_text_occ > 0:
        occ_sub["fullText"] = full_text_occ
    if isinstance(indexing_occ, int) and indexing_occ > 0:
        occ_sub["indexing"] = indexing_occ

    # stfCard/
    stf_card: Dict[str, Any] = {}
    _set_if(stf_card, "localIndex", extracted.get("localIndex"))
    _set_if(stf_card, "caseTitle", case_title)
    _set_if(stf_card, "caseUrl", case_url)
    _set_if(stf_card, "caseClass", _clean_str(extracted.get("caseClass")))
    _set_if(stf_card, "caseNumber", _clean_str(extracted.get("caseNumber")))
    _set_if(stf_card, "judgingBody", _clean_str(extracted.get("judgingBody")))
    _set_if(stf_card, "rapporteur", _clean_str(extracted.get("rapporteur")))
    _set_if(stf_card, "opinionWriter", _clean_str(extracted.get("opinionWriter")))
    _set_if(stf_card, "judgmentDate", judgment_date)
    _set_if(stf_card, "publicationDate", publication_date)
    _set_if(stf_card, "occurrences", occ_sub or None)
    _set_if(stf_card, "domResultContainerId", _clean_str(extracted.get("domResultContainerId")))
    _set_if(stf_card, "domClipboardId", _clean_str(extracted.get("domClipboardId")))

    # identity/
    identity = _subdoc_if_any(
        [
            ("caseCode", case_code),
            ("caseClassDetail", case_class_detail),
            ("caseNumberDetail", case_number_detail),
            # caseDecisionType: "a preencher" -> não cria
            ("stfDecisionId", stf_id),
            ("rawHtmlId", raw_id_str if raw_id_str else None),
        ]
    )

    # dates/
    dates = _subdoc_if_any(
        [
            ("judgmentDate", judgment_date),
            ("publicationDate", publication_date),
        ]
    )

    # caseContent/
    # Regra pedida: se dado não está disponível na fonte, não cria.
    # rawHtml e sanitizedHtml não são fornecidos pelo card: não cria aqui.
    case_content = _subdoc_if_any(
        [
            ("caseUrl", case_url),
        ]
    )

    # audit/
    audit: Dict[str, Any] = {}
    _set_if(audit, "extractionDate", now)
    _set_if(audit, "lastExtractedAt", now)
    _set_if(audit, "builtAt", now)
    _set_if(audit, "updatedAt", now)
    _set_if(audit, "sourceStatus", "extracted")
    _set_if(audit, "pipelineStatus", pipeline_status)

    doc: Dict[str, Any] = {}
    _set_if(doc, "caseTitle", case_title)
    _set_if(doc, "identity", identity)
    _set_if(doc, "dates", dates)
    _set_if(doc, "query", query_sub or None)
    _set_if(doc, "caseContent", case_content)
    _set_if(doc, "stfCard", stf_card or None)
    _set_if(doc, "audit", audit or None)

    return doc


# ------------------------------------------------------------
# Modo de execução
# ------------------------------------------------------------
@dataclass(frozen=True)
class RunPlan:
    option: int
    label: str
    process_only_new_raw: bool
    update_only_existing_decisions: bool
    upsert: bool


PLANS: Dict[int, RunPlan] = {
    1: RunPlan(1, "Processar apenas os registros novos", True,  False, True),
    2: RunPlan(2, "Atualizar registros existentes",        False, True,  False),
    3: RunPlan(3, "Processar todos (sobreescrevendo os existentes)", False, False, True),
}


def choose_action() -> int:
    print("\nEscolha a ação:")
    print("1 - Processar apenas os registros novos")
    print("2 - Atualizar registros existentes")
    print("3 - Processar todos (sobreescrevendo os existentes)")
    print("4 - Abortar")
    while True:
        s = input("Opção: ").strip()
        if s in {"1", "2", "3", "4"}:
            return int(s)
        print("Opção inválida. Digite 1, 2, 3 ou 4.")


# ------------------------------------------------------------
# Totalizadores (origem vs destino)
# ------------------------------------------------------------
def get_processed_raw_ids(dest_col: Collection) -> Set[str]:
    """
    No modelo novo: identity.rawHtmlId é string.
    """
    ids = dest_col.distinct("identity.rawHtmlId")
    return {str(v) for v in ids if v is not None}


def list_source_ids(source_col: Collection) -> List[ObjectId]:
    return [d["_id"] for d in source_col.find({}, projection={"_id": 1}).sort([("_id", 1)])]


def count_source_total(source_col: Collection) -> int:
    return source_col.count_documents({})


def count_source_unprocessed(source_ids: List[ObjectId], processed_ids: Set[str]) -> int:
    return sum(1 for _id in source_ids if str_objectid(_id) not in processed_ids)


# ------------------------------------------------------------
# Claim/finalização raw_html (se houver status)
# ------------------------------------------------------------
def claim_raw_doc(source_col: Collection, raw_id: ObjectId) -> Optional[Dict[str, Any]]:
    raw = source_col.find_one({"_id": raw_id})
    if not raw:
        return None
    if "status" in raw:
        return source_col.find_one_and_update(
            {"_id": raw_id, "status": {"$ne": "extracting"}},
            {"$set": {"status": "extracting", "extractingAt": iso_now()}},
            return_document=ReturnDocument.AFTER,
        )
    return raw


def finalize_raw_status(source_col: Collection, raw_id: ObjectId, status: str, extra: Optional[Dict[str, Any]] = None) -> None:
    upd: Dict[str, Any] = {"status": status, "processedDate": iso_now()}
    if extra:
        upd.update(extra)
    try:
        source_col.update_one({"_id": raw_id}, {"$set": upd})
    except Exception:
        pass


# ------------------------------------------------------------
# Persistência em case_data (compatível com o documento atualizado)
# ------------------------------------------------------------
def upsert_case_data(
    *,
    dest_col: Collection,
    doc: Dict[str, Any],
    plan: RunPlan,
) -> Tuple[str, str]:
    """
    Retorna (dest_id_str, action_str):
    - inserted | updated | skipped
    """
    identity = doc.get("identity") if isinstance(doc.get("identity"), dict) else {}
    stf_id = _clean_str(identity.get("stfDecisionId"))
    if not stf_id:
        return ("", "skipped")

    flt = {"identity.stfDecisionId": stf_id}

    # Se for "atualizar existentes", só atualiza se já existir
    if plan.update_only_existing_decisions:
        existing = dest_col.find_one(flt, projection={"_id": 1})
        if not existing:
            return ("", "skipped")

    now = utc_now()

    # Atualiza somente subpaths de audit para evitar conflitos.
    doc_no_audit = dict(doc)
    doc_no_audit.pop("audit", None)

    update_doc = {
        "$set": {
            **doc_no_audit,
            "audit.updatedAt": now,
            "audit.lastExtractedAt": now,
        },
        "$setOnInsert": {
            "audit.builtAt": now,
            "audit.extractionDate": now,
            "audit.sourceStatus": "extracted",
        },
    }

    result = dest_col.update_one(flt, update_doc, upsert=plan.upsert)

    if result.upserted_id is not None:
        return (str_objectid(result.upserted_id), "inserted")

    got = dest_col.find_one(flt, projection={"_id": 1})
    if got:
        return (str_objectid(got["_id"]), "updated")

    return ("", "skipped")


# ------------------------------------------------------------
# MAIN
# ------------------------------------------------------------
def main() -> None:
    client = get_db_client()
    source_col, dest_col = get_collections(client)

    total_source = count_source_total(source_col)
    source_ids = list_source_ids(source_col)
    processed_raw_ids = get_processed_raw_ids(dest_col)
    total_unprocessed = count_source_unprocessed(source_ids, processed_raw_ids)

    print("============================================================")
    print("TOTALIZADORES")
    print("------------------------------------------------------------")
    print(f"Collection origem ({SOURCE_COLLECTION}) total                  : {total_source}")
    print(f"Collection origem NÃO processados/inseridos em {DEST_COLLECTION}: {total_unprocessed}")
    print("============================================================")

    choice = choose_action()
    if choice == 4:
        print("Abortado.")
        return

    plan = PLANS[choice]
    print(f"\nAção selecionada: {plan.option} - {plan.label}\n")

    # Seleciona quais raw_html serão processados
    if plan.process_only_new_raw:
        selected_ids = [rid for rid in source_ids if str_objectid(rid) not in processed_raw_ids]
    elif plan.update_only_existing_decisions:
        selected_ids = [rid for rid in source_ids if str_objectid(rid) in processed_raw_ids]
    else:
        selected_ids = list(source_ids)

    if not selected_ids:
        print("Nenhum registro elegível para a ação selecionada.")
        return

    extractor = STFListExtractor()

    total_new = 0
    total_updated = 0
    total_skipped = 0
    total_errors = 0

    for raw_id in selected_ids:
        print("-----")
        print(f"Processando registro {raw_id}")

        raw_doc = claim_raw_doc(source_col, raw_id)
        if not raw_doc:
            print("Erro: documento não encontrado.")
            total_errors += 1
            continue

        html = extract_html_from_raw_doc(raw_doc)
        if not html:
            print("Erro: HTML vazio (campo htmlRaw/payload.htmlRaw não encontrado).")
            finalize_raw_status(source_col, raw_id, "error", {"error": "Sem conteúdo HTML"})
            total_errors += 1
            continue

        try:
            decisions = extractor.extract_decisions(html, raw_id)
            print("Extração finalizada")

            if not decisions:
                finalize_raw_status(source_col, raw_id, "empty", {"extractedCount": 0})
                print("Nenhuma decisão encontrada no HTML.")
                continue

            affected_ids: List[str] = []
            inserted = 0
            updated = 0
            skipped = 0

            for d in decisions:
                structured = build_case_data_document(
                    extracted=d,
                    raw_doc=raw_doc,
                    pipeline_status="listExtracted",
                )

                dest_id, action = upsert_case_data(dest_col=dest_col, doc=structured, plan=plan)

                if action == "inserted":
                    inserted += 1
                    if dest_id:
                        affected_ids.append(dest_id)
                elif action == "updated":
                    updated += 1
                    if dest_id:
                        affected_ids.append(dest_id)
                else:
                    skipped += 1

            finalize_raw_status(source_col, raw_id, "extracted", {"extractedCount": len(decisions)})

            print(f"Id(s) gravados/atualizados em {DEST_COLLECTION}: {', '.join(affected_ids) or 'N/A'}")

            total_new += inserted
            total_updated += updated
            total_skipped += skipped

        except Exception as e:
            print(f"Erro na extração/persistência: {e}")
            finalize_raw_status(source_col, raw_id, "error", {"error": str(e)})
            total_errors += 1
            continue

    print("\n============================================================")
    print("RESUMO FINAL")
    print("------------------------------------------------------------")
    print(f"Total de registros novos       : {total_new}")
    print(f"Total de registros atualizados : {total_updated}")
    print(f"Total de registros ignorados   : {total_skipped}")
    print(f"Total de erros                 : {total_errors}")
    print("============================================================")


if __name__ == "__main__":
    try:
        main()
    except PyMongoError as e:
        print(f"[ERRO] MongoDB: {e}")
        raise
