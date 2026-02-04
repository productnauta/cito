#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
g_process_case_html_sanitized.py

Processa caseHtmlSanitized (case_data):
- Loop: buscar documento mais antigo com status="caseSanitized"
- Extrair campos conforme especificação (caseCode, rapporteur, ementa, etc.)
- Atualizar o mesmo documento em case_data com os novos campos
- Alterar status para "caseHtmlProcessed"
- Logar no terminal apenas os eventos solicitados

Dependências:
pip install pymongo beautifulsoup4
"""

import re
import sys
import time
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List, Set

from bs4 import BeautifulSoup
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError
from pymongo import ReturnDocument


# =========================
# Mongo (fixo)
# =========================
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"
COLLECTION = "case_data"

STATUS_INPUT = "caseSanitized"
STATUS_OK = "caseHtmlProcessed"
STATUS_ERROR = "caseHtmlProcessError"
STATUS_PROCESSING = "caseProcessing"


# =========================
# Utils
# =========================
def ts() -> str:
    # "DD-MM-YYY HH:MM:SS"
    return datetime.now().strftime("%d-%m-%Y %H:%M:%S")


def _clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "")).strip()


def _text_with_newlines(node) -> str:
    if not node:
        return ""
    # Mantém quebras úteis, depois normaliza espaços por linha
    raw = node.get_text("\n", strip=True)
    # Normaliza espaços em cada linha, preservando \n
    lines = [re.sub(r"[ \t]+", " ", ln).strip() for ln in raw.splitlines()]
    return "\n".join([ln for ln in lines if ln])


# =========================
# Mongo helpers
# =========================
def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


# def fetch_oldest_to_process(col: Collection) -> Optional[Dict[str, Any]]:
#     return col.find_one({"status": STATUS_INPUT}, sort=[("_id", 1)])
def claim_oldest_to_process(col: Collection) -> Optional[Dict[str, Any]]:
    return col.find_one_and_update(
        {"status": STATUS_INPUT},  # caseSanitized
        {"$set": {"status": STATUS_PROCESSING, "caseHtmlProcessingAt": datetime.now(timezone.utc)}},
        sort=[("_id", 1)],
        return_document=ReturnDocument.AFTER,
    )

""" 
def mark_error(col: Collection, doc_id, *, error_msg: str) -> None:
    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "caseHtmlProcessedAt": datetime.now(timezone.utc),
            "caseHtmlProcessError": error_msg,
            "status": STATUS_ERROR,
        }}
    )
 """
def mark_error(col: Collection, doc_id, *, error_msg: str) -> None:
    col.update_one(
        {"_id": doc_id, "status": STATUS_PROCESSING},
        {"$set": {
            "caseHtmlProcessedAt": datetime.now(timezone.utc),
            "caseHtmlProcessError": error_msg,
            "status": STATUS_ERROR,
        }}
    )

# =========================
# Extraction helpers
# =========================
def _find_header_block(soup: BeautifulSoup):
    """
    Heurística confiável no HTML sanitizado: primeiro div.jud-text que contém "Relator(a):"
    """
    for div in soup.select("div.jud-text"):
        if div.get_text(" ", strip=True).find("Relator(a):") != -1:
            return div
    return None


def _extract_case_code(header_div) -> str:
    if not header_div:
        return ""
    h4s = header_div.find_all("h4")
    if not h4s:
        return ""
    return _clean_text(h4s[0].get_text(" ", strip=True))


def _extract_decision_type(header_div) -> str:
    if not header_div:
        return ""
    h4s = header_div.find_all("h4")
    if len(h4s) < 2:
        return ""
    return _clean_text(h4s[1].get_text(" ", strip=True))


def _extract_labeled_value(header_div, label: str) -> str:
    """
    Procura h4 que começa com "Relator(a):", "Julgamento:", etc.
    """
    if not header_div:
        return ""
    for h4 in header_div.find_all("h4"):
        t = _clean_text(h4.get_text(" ", strip=True))
        if t.startswith(label):
            return _clean_text(t.split(":", 1)[1] if ":" in t else t.replace(label, "").strip())
    return ""


def _extract_from_case_code(case_code: str) -> Dict[str, str]:
    """
    De caseCode extrai:
    - caseClassDetail
    - caseNumberDetail
    - caseUfDetail
    """
    out = {"caseClassDetail": "", "caseNumberDetail": "", "caseUfDetail": ""}

    if not case_code:
        return out

    # Ex.: "ADPF 1159 MC-Ref / SC - SANTA CATARINA"
    left = case_code
    uf = ""
    if "/" in case_code:
        parts = case_code.split("/", 1)
        left = parts[0].strip()
        uf = parts[1].strip()

    out["caseUfDetail"] = uf

    m_class = re.match(r"^([A-Z]+)\s+", left)
    if m_class:
        out["caseClassDetail"] = m_class.group(1).strip()

    m_num = re.match(r"^[A-Z]+\s+(\d[\d\.\-]*)", left)
    if m_num:
        out["caseNumberDetail"] = m_num.group(1).strip()

    return out


def _section_container_for_title(soup: BeautifulSoup, title: str):
    """
    Encontra um bloco div.jud-text que possua um h4 exatamente igual ao título.
    Ex.: "Publicação", "Partes", "Indexação", etc.
    """
    for div in soup.select("div.jud-text"):
        h4 = div.find("h4")
        if h4 and _clean_text(h4.get_text(" ", strip=True)) == title:
            return div
    return None


def _extract_text_pre_wrap_section(soup: BeautifulSoup, title: str) -> str:
    div = _section_container_for_title(soup, title)
    if not div:
        return ""
    tpw = div.select_one("div.text-pre-wrap")
    return _text_with_newlines(tpw) if tpw else ""


def _extract_next_div_section(soup: BeautifulSoup, title: str) -> str:
    """
    Para "Ementa" e "Decisão" (no HTML sanitizado) o texto costuma estar no div irmão seguinte do h4.
    """
    div = _section_container_for_title(soup, title)
    if not div:
        return ""
    h4 = div.find("h4")
    if not h4:
        return ""
    nxt = h4.find_next_sibling("div")
    return _text_with_newlines(nxt) if nxt else ""


def _extract_ui_tooltips(soup: BeautifulSoup) -> List[str]:
    tooltips: Set[str] = set()
    for tag in soup.find_all(attrs={"mattooltip": True}):
        val = (tag.get("mattooltip") or "").strip()
        if val:
            tooltips.add(val)
    return sorted(tooltips)


def _extract_ods_tags(soup: BeautifulSoup) -> List[str]:
    out: List[str] = []
    imgs = soup.select('a[mattooltip="Conheça a Agenda 2030 da ONU"] img')
    for img in imgs:
        alt = (img.get("alt") or "").strip()
        src = (img.get("src") or "").strip()
        if alt:
            out.append(alt)
        elif src:
            out.append(src)
    # remove duplicatas preservando ordem
    seen = set()
    uniq = []
    for x in out:
        if x not in seen:
            seen.add(x)
            uniq.append(x)
    return uniq


# =========================
# Core extraction
# =========================
def extract_all_fields(case_html_sanitized: str) -> Dict[str, Any]:
    soup = BeautifulSoup(case_html_sanitized, "html.parser")

    header_div = _find_header_block(soup)

    case_code = _extract_case_code(header_div)
    derived = _extract_from_case_code(case_code)

    data: Dict[str, Any] = {}

    # Cabeçalho / IDs
    data["caseCode"] = case_code
    data["caseDecisionType"] = _extract_decision_type(header_div)

    # Labels do header
    data["rapporteur"] = _extract_labeled_value(header_div, "Relator(a):")
    data["judgmentDate"] = _extract_labeled_value(header_div, "Julgamento:")
    data["publicationDate"] = _extract_labeled_value(header_div, "Publicação:")
    data["judgingBody"] = _extract_labeled_value(header_div, "Órgão julgador:")

    # Derivados do caseCode
    data.update(derived)

    # UI/tooltips e ODS
    data["uiTooltips"] = _extract_ui_tooltips(soup)
    data["odsTags"] = _extract_ods_tags(soup)

    # Blocos text-pre-wrap
    data["publicationBlock"] = _extract_text_pre_wrap_section(soup, "Publicação")
    data["partiesBlock"] = _extract_text_pre_wrap_section(soup, "Partes")
    data["indexingText"] = _extract_text_pre_wrap_section(soup, "Indexação")
    data["legislationText"] = _extract_text_pre_wrap_section(soup, "Legislação")
    data["observationText"] = _extract_text_pre_wrap_section(soup, "Observação")
    data["similarCasesBlock"] = _extract_text_pre_wrap_section(soup, "Acórdãos no mesmo sentido")
    data["doctrineBlock"] = _extract_text_pre_wrap_section(soup, "Doutrina")

    # Blocos (div irmão do h4)
    data["ementaText"] = _extract_next_div_section(soup, "Ementa")
    data["decisionText"] = _extract_next_div_section(soup, "Decisão")

    # Remove chaves vazias (mantém listas vazias? remove também)
    cleaned: Dict[str, Any] = {}
    for k, v in data.items():
        if isinstance(v, list):
            if v:  # só grava se houver itens
                cleaned[k] = v
        else:
            if (v or "").strip():
                cleaned[k] = v.strip()

    return cleaned


# =========================
# Loop principal (status=caseSanitized)
# =========================
def run_loop() -> int:
    col = get_collection()
    total = 0

    while True:
#        doc = fetch_oldest_to_process(col)
        doc = claim_oldest_to_process(col)

        if not doc:
            break

        start = time.time()

        doc_id = doc["_id"]
        title = (doc.get("caseTitle") or doc.get("caseCode") or "Sem título").strip()

        print(f"{ts()} - Iniciando processamento do documento '{doc_id}': '{title}'")

        try:
            html_sanitized = (doc.get("caseHtmlSanitized") or "").strip()
            if not html_sanitized:
                raise ValueError("Documento não possui 'caseHtmlSanitized' preenchido.")

            extracted = extract_all_fields(html_sanitized)

            # Atualiza documento com campos extraídos + status final
            update_fields = dict(extracted)
            update_fields["caseHtmlProcessedAt"] = datetime.now(timezone.utc)
            update_fields["status"] = STATUS_OK

            #col.update_one({"_id": doc_id}, {"$set": update_fields})
            col.update_one(
                {"_id": doc_id, "status": STATUS_PROCESSING},
                {"$set": update_fields}
            )


            elapsed = time.time() - start
            total += 1

            print(f"{ts()} - Extração concluída para o documento '{doc_id}': '{title}'")
            print(f"{ts()} - Dados obtidos:")
            for field_name in sorted(extracted.keys()):
                print(f"    - {field_name}")
            # se não extraiu nada, ainda imprime a lista vazia (conforme requisito: listar nomes)
            if not extracted:
                print("    - (nenhum campo extraído)")

            # tempo
            if elapsed >= 60:
                mins = elapsed / 60.0
                tempo_str = f"{mins:.2f} minutos"
            else:
                tempo_str = f"{elapsed:.2f} segundos"

            print(f"{ts()} - Tempo total de processamento: '{tempo_str}'")
            print(f"{ts()} - Status final: '{STATUS_OK}'")

        except Exception as e:
            mark_error(col, doc_id, error_msg=str(e))
            # Requisito não pede log de erro. Mantido silencioso no terminal.

    return 0


if __name__ == "__main__":
    try:
        sys.exit(run_loop())
    except PyMongoError:
        # sem logs extras conforme requisito
        sys.exit(2)
    except Exception:
        sys.exit(1)
