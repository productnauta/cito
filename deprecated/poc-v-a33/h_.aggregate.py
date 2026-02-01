#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
h_build_case_index_collection.py

Fluxo (interativo)
1) Lista TODOS os registros elegíveis em case_data (por status, se habilitado).
2) Verifica quais ainda NÃO existem na collection destino (por stfDecisionId).
3) Para cada registro pendente:
   - exibe resumo do que será inserido
   - solicita confirmação (y/N) para inserir
   - consolida (case_data + raw_html.queryString via sourceDocumentId)
   - faz upsert na collection destino
4) Ao final, informa total inserido.

Estrutura do documento destino (conforme solicitado)
- _id (gerado pelo Mongo da collection destino)
- stfDecisionId
- caseDataId
- rawHtmlId
- sourceStatus
- caseData: { queryString, caseCode, caseClassDetail, caseNumberDetail, caseDecisionType, judgmentDate, publicationDate }
- stfData:  { caseNumber, caseTitle, caseClass, judgingBody, rapporteur, caseUrl }
- caseHtmlProcessedAt
- createdAt
- builtAt

ENV
- TARGET_COLLECTION: nome da collection destino (default: case_index)
- FILTER_STATUS: filtra docs por case_data.status (default: caseHtmlProcessed; vazio desabilita)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from bson import ObjectId
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# ------------------------------------------------------------
# MONGO (iguais ao projeto)
# ------------------------------------------------------------
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"

DB_NAME = "cito-v-a33-240125"
RAW_HTML_COLLECTION = "raw_html"
CASE_DATA_COLLECTION = "case_data"

# NOVA collection exclusiva
TARGET_COLLECTION = os.getenv("TARGET_COLLECTION", "case_index")

# (opcional) processar apenas docs finalizados
FILTER_STATUS = os.getenv("FILTER_STATUS", "caseHtmlProcessed")  # use "" para desabilitar


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------
def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _as_object_id(v: Any) -> Optional[ObjectId]:
    if v is None:
        return None
    if isinstance(v, ObjectId):
        return v
    s = str(v).strip()
    if not s:
        return None
    try:
        return ObjectId(s)
    except Exception:
        return None


def _get_collections() -> Tuple[Collection, Collection, Collection]:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[RAW_HTML_COLLECTION], db[CASE_DATA_COLLECTION], db[TARGET_COLLECTION]


def ensure_indexes(target: Collection) -> None:
    # Chave lógica única
    target.create_index([("stfDecisionId", 1)], unique=True, name="ux_stfDecisionId")

    # Índices úteis (nested também)
    target.create_index([("caseDataId", 1)], name="ix_caseDataId")
    target.create_index([("rawHtmlId", 1)], name="ix_rawHtmlId")
    target.create_index([("caseData.queryString", 1)], name="ix_caseData_queryString")
    target.create_index([("stfData.judgingBody", 1)], name="ix_stfData_judgingBody")
    target.create_index([("stfData.rapporteur", 1)], name="ix_stfData_rapporteur")
    target.create_index([("caseData.caseClassDetail", 1)], name="ix_caseData_caseClassDetail")
    target.create_index([("caseData.caseNumberDetail", 1)], name="ix_caseData_caseNumberDetail")


def build_consolidated_doc(*, case_doc: Dict[str, Any], raw_doc: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Monta o documento consolidado no formato com agrupadores:
    - caseData (canônico/pós-sanitização)
    - stfData (listagem STF)
    """
    case_data_id = case_doc.get("_id")
    raw_id = raw_doc.get("_id") if raw_doc else None
    query_string = (raw_doc.get("queryString") if raw_doc else None)

    out: Dict[str, Any] = {
        # raiz: ids e controle
        "stfDecisionId": case_doc.get("stfDecisionId"),
        "caseDataId": case_data_id,
        "rawHtmlId": raw_id,
        "sourceStatus": case_doc.get("status"),

        # agrupador: dados canônicos/jurídicos
        "caseData": {
            "queryString": query_string,
            "caseCode": case_doc.get("caseCode"),
            "caseClassDetail": case_doc.get("caseClassDetail"),
            "caseNumberDetail": case_doc.get("caseNumberDetail"),
            "caseDecisionType": case_doc.get("caseDecisionType"),
            "judgmentDate": case_doc.get("judgmentDate"),
            "publicationDate": case_doc.get("publicationDate"),
        },

        # agrupador: dados do STF (listagem)
        "stfData": {
            "caseNumber": case_doc.get("caseNumber"),
            "caseTitle": case_doc.get("caseTitle"),
            "caseClass": case_doc.get("caseClass"),
            "judgingBody": case_doc.get("judgingBody"),
            "rapporteur": case_doc.get("rapporteur"),
            "caseUrl": case_doc.get("caseUrl"),
        },

        # metadados do pipeline
        "caseHtmlProcessedAt": case_doc.get("caseHtmlProcessedAt"),

        # builtAt será atualizado em cada rebuild/execução
        "builtAt": _utc_now(),
    }

    # limpeza: remove None e strings vazias RECUSIVAMENTE (mantendo estrutura)
    def _clean(obj: Any) -> Any:
        if isinstance(obj, dict):
            cleaned_dict: Dict[str, Any] = {}
            for k, v in obj.items():
                cv = _clean(v)
                if cv is None:
                    continue
                # remove string vazia
                if isinstance(cv, str) and not cv.strip():
                    continue
                # remove dict vazio
                if isinstance(cv, dict) and not cv:
                    continue
                cleaned_dict[k] = cv
            return cleaned_dict
        if isinstance(obj, list):
            cleaned_list = [x for x in (_clean(v) for v in obj) if x is not None]
            return cleaned_list
        return obj

    cleaned = _clean(out)

    # validação mínima
    if not cleaned.get("stfDecisionId"):
        raise ValueError("Documento consolidado sem stfDecisionId.")

    return cleaned


def upsert_target(target: Collection, doc: Dict[str, Any]) -> None:
    stf_id = doc["stfDecisionId"]

    target.update_one(
        {"stfDecisionId": stf_id},
        {
            "$set": doc,
            "$setOnInsert": {"createdAt": _utc_now()},
        },
        upsert=True,
    )


def list_eligible_case_docs(case_col: Collection) -> List[Dict[str, Any]]:
    query: Dict[str, Any] = {"stfDecisionId": {"$exists": True, "$nin": [None, "", "N/A"]}}
    if FILTER_STATUS:
        query["status"] = FILTER_STATUS

    projection = {
        "_id": 1,
        "stfDecisionId": 1,
        "status": 1,
        "sourceDocumentId": 1,
        "caseHtmlProcessedAt": 1,
        # STF/listagem
        "caseTitle": 1,
        "caseUrl": 1,
        "caseClass": 1,
        "caseNumber": 1,
        "judgingBody": 1,
        "rapporteur": 1,
        # Canônico/pós-sanitização
        "caseCode": 1,
        "caseClassDetail": 1,
        "caseNumberDetail": 1,
        "caseDecisionType": 1,
        "judgmentDate": 1,
        "publicationDate": 1,
    }

    return list(case_col.find(query, projection=projection).sort([("_id", 1)]))


def fetch_raw_doc_and_query_string(raw_col: Collection, case_doc: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    raw_oid = _as_object_id(case_doc.get("sourceDocumentId"))
    if not raw_oid:
        return None, None
    raw_doc = raw_col.find_one({"_id": raw_oid}, projection={"_id": 1, "queryString": 1})
    return raw_doc, (raw_doc.get("queryString") if raw_doc else None)


def compute_pending_stf_ids(target_col: Collection, stf_ids: List[str]) -> List[str]:
    if not stf_ids:
        return []
    existing = set(
        d["stfDecisionId"]
        for d in target_col.find({"stfDecisionId": {"$in": stf_ids}}, projection={"stfDecisionId": 1})
    )
    return [x for x in stf_ids if x not in existing]


def confirm(prompt: str) -> bool:
    ans = input(prompt).strip().lower()
    return ans in ("y", "yes", "s", "sim")


def summarize_consolidated(doc: Dict[str, Any]) -> str:
    case_data = doc.get("caseData", {}) or {}
    stf_data = doc.get("stfData", {}) or {}
    return (
        f"stfDecisionId : {doc.get('stfDecisionId')}\n"
        f"caseDataId    : {doc.get('caseDataId')}\n"
        f"rawHtmlId     : {doc.get('rawHtmlId')}\n"
        f"sourceStatus  : {doc.get('sourceStatus')}\n"
        f"caseData:\n"
        f"  queryString      : {case_data.get('queryString')}\n"
        f"  caseCode         : {case_data.get('caseCode')}\n"
        f"  caseClassDetail  : {case_data.get('caseClassDetail')}\n"
        f"  caseNumberDetail : {case_data.get('caseNumberDetail')}\n"
        f"  caseDecisionType : {case_data.get('caseDecisionType')}\n"
        f"  judgmentDate     : {case_data.get('judgmentDate')}\n"
        f"  publicationDate  : {case_data.get('publicationDate')}\n"
        f"stfData:\n"
        f"  caseTitle    : {stf_data.get('caseTitle')}\n"
        f"  caseClass    : {stf_data.get('caseClass')}\n"
        f"  caseNumber   : {stf_data.get('caseNumber')}\n"
        f"  judgingBody  : {stf_data.get('judgingBody')}\n"
        f"  rapporteur   : {stf_data.get('rapporteur')}\n"
        f"  caseUrl      : {stf_data.get('caseUrl')}\n"
        f"caseHtmlProcessedAt: {doc.get('caseHtmlProcessedAt')}"
    )


def run() -> int:
    raw_col, case_col, target_col = _get_collections()
    ensure_indexes(target_col)

    eligible = list_eligible_case_docs(case_col)

    print("============================================================")
    print("1) LISTAGEM DOS REGISTROS ELEGÍVEIS (case_data)")
    print("------------------------------------------------------------")
    print(f"Filtro status: {FILTER_STATUS!r} (vazio = desabilitado)")
    print(f"Total elegíveis: {len(eligible)}")
    print("============================================================")

    if not eligible:
        print("[OK] Nenhum registro elegível.")
        return 0

    for i, doc in enumerate(eligible, start=1):
        print(f"[{i}/{len(eligible)}] stfDecisionId={doc.get('stfDecisionId')} | title={doc.get('caseTitle')!r}")

    stf_ids = [d["stfDecisionId"] for d in eligible if d.get("stfDecisionId")]
    pending_ids = compute_pending_stf_ids(target_col, stf_ids)

    print("\n============================================================")
    print("2) VERIFICAÇÃO DE PENDÊNCIAS (case_index)")
    print("------------------------------------------------------------")
    print(f"Collection destino: {TARGET_COLLECTION}")
    print(f"Já existentes      : {len(stf_ids) - len(pending_ids)}")
    print(f"PENDENTES          : {len(pending_ids)}")
    print("============================================================")

    if not pending_ids:
        print("[OK] Nada a inserir (todos já existem na collection destino).")
        return 0

    eligible_by_id = {d["stfDecisionId"]: d for d in eligible if d.get("stfDecisionId")}

    inserted = 0
    skipped = 0
    errors = 0

    print("\n============================================================")
    print("3) INSERÇÃO SOB CONFIRMAÇÃO (POR REGISTRO)")
    print("============================================================")

    for idx, stf_id in enumerate(pending_ids, start=1):
        case_doc = eligible_by_id.get(stf_id)
        if not case_doc:
            continue

        raw_doc, _query_string = fetch_raw_doc_and_query_string(raw_col, case_doc)

        try:
            consolidated = build_consolidated_doc(case_doc=case_doc, raw_doc=raw_doc)
        except Exception as e:
            errors += 1
            print("\n------------------------------------------------------------")
            print(f"[PENDENTE {idx}/{len(pending_ids)}] stfDecisionId={stf_id}")
            print(f"-> ERRO ao montar documento: {e}")
            continue

        print("\n------------------------------------------------------------")
        print(f"[PENDENTE {idx}/{len(pending_ids)}]")
        print(summarize_consolidated(consolidated))
        print("------------------------------------------------------------")

        if not confirm("Inserir este registro em case_index? [y/N]: "):
            print("-> Pulado (não confirmado).")
            skipped += 1
            continue

        try:
            upsert_target(target_col, consolidated)
            inserted += 1
            print("-> Inserido/atualizado com sucesso.")
        except Exception as e:
            errors += 1
            print(f"-> ERRO ao inserir: {e}")

    print("\n============================================================")
    print("RESULTADO FINAL")
    print("------------------------------------------------------------")
    print(f"Elegíveis : {len(eligible)}")
    print(f"Pendentes : {len(pending_ids)}")
    print(f"Inseridos : {inserted}")
    print(f"Pulados   : {skipped}")
    print(f"Erros     : {errors}")
    print("============================================================")

    return 0 if errors == 0 else 1


if __name__ == "__main__":
    try:
        raise SystemExit(run())
    except PyMongoError as e:
        print(f"[ERRO] MongoDB: {e}")
        raise SystemExit(2)
    except KeyboardInterrupt:
        print("\n[ABORTADO] Execução interrompida pelo usuário.")
        raise SystemExit(130)
    except Exception as e:
        print(f"[ERRO] Geral: {e}")
        raise SystemExit(1)
