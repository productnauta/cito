#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step06-extract-legislation-python.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts legislation references from caseContent.md.legislation and stores in caseData.
Inputs: config/mongo.json, caseContent.md.legislation.
Outputs: caseData.legislationReferences  processing/status updates.
Pipeline: load document -> parse legislation -> persist references.
Dependencies: pymongo
------------------------------------------------------------

"""

from __future__ import annotations

import json
import re
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# =============================================================================
# 0) LOG / TIME
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# 1) CONFIG (Mongo)
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"

CASE_DATA_COLLECTION = "case_data"

OUTPUT_PIPELINE_STATUS = "legislationExtracted"
ERROR_PIPELINE_STATUS = "legislationExtractError"


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


def load_json(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida.")

    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()

    if not uri or not db:
        raise ValueError("Config inválida: 'mongo.uri' ou 'mongo.database' vazio.")

    return MongoCfg(uri=uri, database=db)


def get_case_data_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    cfg = build_mongo_cfg(load_json(MONGO_CONFIG_PATH))

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)
    client.admin.command("ping")

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) EXTRAÇÃO DE LEGISLAÇÃO
# =============================================================================

def roman_to_int(roman: str) -> int:
    """Converte algarismos romanos para inteiro."""
    roman_dict = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    result = 0
    prev_value = 0

    for char in reversed(roman.upper()):
        value = roman_dict.get(char, 0)
        if value < prev_value:
            result -= value
        else:
            result = value
        prev_value = value

    return result


def extract_legislation_data(text: str) -> Dict[str, Any]:
    """
    Extrai dados de legislação do texto seguindo as regras especificadas.
    """
    legislation_map: Dict[str, Any] = {}

    # Padrões de regex
    norm_pattern = r'LEG-(\w)\s(\w)-?(\d)?\sANO-(\d{4})'
    article_pattern = r'ART-(\d)'
    inciso_pattern = r'INC-(\d)'
    paragraph_pattern = r'PAR-(\d)'
    letter_pattern = r'LET-(\w)'
    caput_pattern = r'"CAPUT"'

    lines = (text or "").split('\n')
    current_norm = None
    current_article = None
    pending_incisos: List[int] = []
    pending_paragraphs: List[int] = []
    pending_letter = None
    has_caput = False

    for line in lines:
        # Procura por nova norma
        norm_match = re.search(norm_pattern, line)
        if norm_match:
            jurisdiction = norm_match.group(1).lower()
            norm_prefix = norm_match.group(2)
            norm_number = norm_match.group(3) if norm_match.group(3) else ""
            norm_year = int(norm_match.group(4))

            # Mapeia tipos de norma
            norm_type_map = {
                'CF': 'CF',
                'EC': 'EC',
                'LC': 'LC',
                'LEI': 'LEI',
                'DECRETO': 'DECRETO',
                'DEC': 'DECRETO',
                'RESOLUCAO': 'RESOLUCAO',
                'PORTARIA': 'PORTARIA',
                'ADCT': 'OUTRA',
                'DEL': 'OUTRA',
                'DLG': 'OUTRA',
                'PJL': 'OUTRA',
                'CVC': 'OUTRA'
            }

            norm_type = norm_type_map.get(norm_prefix, 'OUTRA')

            # Gera identificador
            if norm_number:
                norm_identifier = f"{norm_prefix}-{norm_number}-{norm_year}"
            else:
                norm_identifier = f"{norm_prefix}-{norm_year}"

            # Extrai descrição
            description_match = re.search(
                r'(?:'  re.escape(f'ANO-{norm_year}')  r'\s)(.*?)(?=LEG-|$)',
                line,
                re.DOTALL
            )
            norm_description = description_match.group(1).strip() if description_match else ""

            # Limpa a descrição removendo tags de artigos, incisos, etc
            norm_description = re.sub(
                r'\s*(ART-\d|INC-\d|PAR-\d|LET-\w|"CAPUT")\s*',
                ' ',
                norm_description
            ).strip()

            # Determina jurisdição
            jurisdiction_level = "federal" if jurisdiction == "fed" else "unknown"

            current_norm = norm_identifier

            if current_norm not in legislation_map:
                legislation_map[current_norm] = {
                    "normIdentifier": norm_identifier,
                    "jurisdictionLevel": jurisdiction_level,
                    "normType": norm_type,
                    "normYear": norm_year,
                    "normDescription": norm_description,
                    "normReferences": []
                }

            current_article = None
            pending_incisos = []
            pending_paragraphs = []
            pending_letter = None
            has_caput = False

        # Verifica CAPUT
        if re.search(caput_pattern, line):
            has_caput = True

        # Procura por artigos
        for art_match in re.finditer(article_pattern, line):
            # Salva referência anterior se existir
            if current_article is not None and current_norm:
                _save_reference(
                    legislation_map,
                    current_norm,
                    current_article,
                    pending_incisos,
                    pending_paragraphs,
                    pending_letter,
                    has_caput,
                )

            current_article = int(art_match.group(1))
            pending_incisos = []
            pending_paragraphs = []
            pending_letter = None
            has_caput = False

        # Procura por incisos
        for inc_match in re.finditer(inciso_pattern, line):
            pending_incisos.append(int(inc_match.group(1)))

        # Procura por parágrafos
        for par_match in re.finditer(paragraph_pattern, line):
            pending_paragraphs.append(int(par_match.group(1)))

        # Procura por letra
        let_match = re.search(letter_pattern, line)
        if let_match:
            pending_letter = let_match.group(1).lower()

    # Salva última referência
    if current_article is not None and current_norm:
        _save_reference(
            legislation_map,
            current_norm,
            current_article,
            pending_incisos,
            pending_paragraphs,
            pending_letter,
            has_caput,
        )

    return {
        "caseData": {
            "legislationReferences": list(legislation_map.values())
        }
    }


def _save_reference(
    legislation_map: Dict[str, Any],
    norm_key: str,
    article: int,
    incisos: List[int],
    paragraphs: List[int],
    letter: str | None,
    has_caput: bool,
) -> None:
    """Salva uma referência normativa."""
    if not incisos and not paragraphs and not letter:
        # Apenas artigo - é caput
        ref = {
            "articleNumber": article,
            "isCaput": True,
            "incisoNumber": None,
            "paragraphNumber": None,
            "isParagraphSingle": False,
            "letterCode": None
        }
        legislation_map[norm_key]["normReferences"].append(ref)
    else:
        # Tem incisos
        if incisos:
            for inciso in incisos:
                ref = {
                    "articleNumber": article,
                    "isCaput": False,
                    "incisoNumber": inciso,
                    "paragraphNumber": None,
                    "isParagraphSingle": False,
                    "letterCode": letter
                }
                legislation_map[norm_key]["normReferences"].append(ref)

        # Tem parágrafos
        if paragraphs:
            for paragraph in paragraphs:
                ref = {
                    "articleNumber": article,
                    "isCaput": has_caput,
                    "incisoNumber": None,
                    "paragraphNumber": paragraph,
                    "isParagraphSingle": False,
                    "letterCode": None
                }
                legislation_map[norm_key]["normReferences"].append(ref)

        # Tem letra sem inciso nem parágrafo
        if letter and not incisos and not paragraphs:
            ref = {
                "articleNumber": article,
                "isCaput": False,
                "incisoNumber": None,
                "paragraphNumber": None,
                "isParagraphSingle": False,
                "letterCode": letter
            }
            legislation_map[norm_key]["normReferences"].append(ref)


# =============================================================================
# 3) PROCESSAMENTO ÚNICO
# =============================================================================

def persist_success(col: Collection, doc_id: Any, refs: List[Dict[str, Any]]) -> None:
    update = {
        "caseData.legislationReferences": refs,
        "processing.caseLegislationRefsStatus": "success",
        "processing.caseLegislationRefsError": None,
        "processing.caseLegislationRefsAt": utc_now(),
        "processing.caseLegislationRefsProvider": "python",
        "processing.caseLegislationRefsModel": "regex",
        "processing.pipelineStatus": OUTPUT_PIPELINE_STATUS,
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": OUTPUT_PIPELINE_STATUS,
        "status.updatedAt": utc_now(),
    }
    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(col: Collection, doc_id: Any, err: str) -> None:
    update = {
        "processing.caseLegislationRefsStatus": "error",
        "processing.caseLegislationRefsError": err,
        "processing.caseLegislationRefsAt": utc_now(),
        "processing.caseLegislationRefsProvider": "python",
        "processing.caseLegislationRefsModel": "regex",
        "processing.pipelineStatus": ERROR_PIPELINE_STATUS,
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": ERROR_PIPELINE_STATUS,
        "status.updatedAt": utc_now(),
    }
    col.update_one({"_id": doc_id}, {"$set": update})


def process_document(col: Collection, stf_decision_id: str) -> int:
    log("STEP", f"Buscando documento identity.stfDecisionId='{stf_decision_id}'")

    doc = col.find_one({"identity.stfDecisionId": stf_decision_id})
    if not doc:
        log("ERROR", "Documento não encontrado.")
        return 1

    doc_id = doc.get("_id")
    md_node = (doc.get("caseContent") or {}).get("md") or {}
    md_legislation = md_node.get("legislation") or ""

    log("STEP", "Extraindo referências de legislação")
    extracted = extract_legislation_data(md_legislation)
    refs = (extracted.get("caseData") or {}).get("legislationReferences") or []

    persist_success(col, doc_id, refs)

    log("OK", f"Processamento concluído | referencias={len(refs)}")
    return 0


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "ETAPA: EXTRAIR LEGISLAÇÃO (PYTHON)")

    try:
        col = get_case_data_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId não informado.")
        return 1

    try:
        return process_document(col, stf_decision_id)
    except Exception as e:
        log("ERROR", f"Erro fatal: {type(e).__name__}: {e}")
        doc = col.find_one({"identity.stfDecisionId": stf_decision_id})
        if doc and doc.get("_id"):
            persist_error(col, doc["_id"], str(e))
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        log("WARN", "Interrompido pelo usuário.")
        sys.exit(130)
    except PyMongoError as e:
        log("ERROR", f"MongoDB erro: {e}")
        sys.exit(2)

    for char in reversed(roman.upper()):
        value = roman_dict.get(char, 0)
        if value < prev_value:
            result -= value
        else:
            result = value
        prev_value = value
    
    return result

def extract_legislation_data(text: str) -> Dict:
    """
    Extrai dados de legislação do texto seguindo as regras especificadas.
    """
    legislation_map = {}
    
    # Padrões de regex
    norm_pattern = r'LEG-(\w)\s(\w)-?(\d)?\sANO-(\d{4})'
    article_pattern = r'ART-(\d)'
    inciso_pattern = r'INC-(\d)'       
    paragraph_pattern = r'PAR-(\d)'
    letter_pattern = r'LET-(\w)'
    caput_pattern = r'"CAPUT"'
    
    lines = text.split('\n')
    current_norm = None
    current_article = None
    pending_incisos = []
    pending_paragraphs = []
    pending_letter = None
    has_caput = False
    
    for line in lines:
        # Procura por nova norma
        norm_match = re.search(norm_pattern, line)
        if norm_match:
            jurisdiction = norm_match.group(1).lower()
            norm_prefix = norm_match.group(2)
            norm_number = norm_match.group(3) if norm_match.group(3) else ""
            norm_year = int(norm_match.group(4))
            
            # Mapeia tipos de norma
            norm_type_map = {
                'CF': 'CF',
                'EC': 'EC',
                'LC': 'LC',
                'LEI': 'LEI',
                'DECRETO': 'DECRETO',
                'DEC': 'DECRETO',
                'RESOLUCAO': 'RESOLUCAO',
                'PORTARIA': 'PORTARIA',
                'ADCT': 'OUTRA',
                'DEL': 'OUTRA',
                'DLG': 'OUTRA',
                'PJL': 'OUTRA',
                'CVC': 'OUTRA'
            }
            
            norm_type = norm_type_map.get(norm_prefix, 'OUTRA')
            
            # Gera identificador
            if norm_number:
                norm_identifier = f"{norm_prefix}-{norm_number}-{norm_year}"
            else:
                norm_identifier = f"{norm_prefix}-{norm_year}"
            
            # Extrai descrição
            description_match = re.search(r'(?:'  re.escape(f'ANO-{norm_year}')  r'\s)(.*?)(?=LEG-|$)', line, re.DOTALL)
            norm_description = description_match.group(1).strip() if description_match else ""
            
            # Limpa a descrição removendo tags de artigos, incisos, etc
            norm_description = re.sub(r'\s*(ART-\d|INC-\d|PAR-\d|LET-\w|"CAPUT")\s*', ' ', norm_description).strip()
            
            # Determina jurisdição
            jurisdiction_level = "federal" if jurisdiction == "fed" else "unknown"
            
            current_norm = norm_identifier
            
            if current_norm not in legislation_map:
                legislation_map[current_norm] = {
                    "normIdentifier": norm_identifier,
                    "jurisdictionLevel": jurisdiction_level,
                    "normType": norm_type,
                    "normYear": norm_year,
                    "normDescription": norm_description,
                    "normReferences": []
                }
            
            current_article = None
            pending_incisos = []
            pending_paragraphs = []
            pending_letter = None
            has_caput = False
        
        # Verifica CAPUT
        if re.search(caput_pattern, line):
            has_caput = True
        
        # Procura por artigos
        for art_match in re.finditer(article_pattern, line):
            # Salva referência anterior se existir
            if current_article is not None and current_norm:
                _save_reference(legislation_map, current_norm, current_article, 
                              pending_incisos, pending_paragraphs, pending_letter, has_caput)
            
            current_article = int(art_match.group(1))
            pending_incisos = []
            pending_paragraphs = []
            pending_letter = None
            has_caput = False
        
        # Procura por incisos
        for inc_match in re.finditer(inciso_pattern, line):
            pending_incisos.append(int(inc_match.group(1)))
        
        # Procura por parágrafos
        for par_match in re.finditer(paragraph_pattern, line):
            pending_paragraphs.append(int(par_match.group(1)))
        
        # Procura por letra
        let_match = re.search(letter_pattern, line)
        if let_match:
            pending_letter = let_match.group(1).lower()
    
    # Salva última referência
    if current_article is not None and current_norm:
        _save_reference(legislation_map, current_norm, current_article, 
                       pending_incisos, pending_paragraphs, pending_letter, has_caput)
    
    # Converte para formato final
    result = {
        "caseData": {
            "legislationReferences": list(legislation_map.values())
        }
    }
    
    return result

def _save_reference(legislation_map, norm_key, article, incisos, paragraphs, letter, has_caput):
    """Salva uma referência normativa."""
    if not incisos and not paragraphs and not letter:
        # Apenas artigo - é caput
        ref = {
            "articleNumber": article,
            "isCaput": True,
            "incisoNumber": None,
            "paragraphNumber": None,
            "isParagraphSingle": False,
            "letterCode": None
        }
        legislation_map[norm_key]["normReferences"].append(ref)
    else:
        # Tem incisos
        if incisos:
            for inciso in incisos:
                ref = {
                    "articleNumber": article,
                    "isCaput": False,
                    "incisoNumber": inciso,
                    "paragraphNumber": None,
                    "isParagraphSingle": False,
                    "letterCode": letter
                }
                legislation_map[norm_key]["normReferences"].append(ref)
        
        # Tem parágrafos
        if paragraphs:
            for paragraph in paragraphs:
                ref = {
                    "articleNumber": article,
                    "isCaput": has_caput,
                    "incisoNumber": None,
                    "paragraphNumber": paragraph,
                    "isParagraphSingle": False,
                    "letterCode": None
                }
                legislation_map[norm_key]["normReferences"].append(ref)
        
        # Tem letra sem inciso nem parágrafo
        if letter and not incisos and not paragraphs:
            ref = {
                "articleNumber": article,
                "isCaput": False,
                "incisoNumber": None,
                "paragraphNumber": None,
                "isParagraphSingle": False,
                "letterCode": letter
            }
            legislation_map[norm_key]["normReferences"].append(ref)

# Uso
texto = """LEG-FED CF ANO-1988 ART-00001 INC-00003..."""

resultado = extract_legislation_data(texto)
print(json.dumps(resultado, indent=2, ensure_ascii=False))