#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
j_extract_doctrine_refs_mistral.py

Extrai referências doutrinárias de caseData.caseDoctrine usando Mistral API.
- Lista documentos com status.pipelineStatus="caseScraped"
- Permite processar todos ou um a um
- Persiste caseData.caseDoctrineReferences (quando houver)

Dependências:
pip install pymongo requests
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests
from pymongo import MongoClient
from pymongo.collection import Collection


# =========================
# Config
# =========================
MONGO_USER = "cito"
MONGO_PASS = "fyu9WxkHakGKHeoq"
MONGO_URI = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@cluster0.gb8bzlp.mongodb.net/?appName=Cluster0"
DB_NAME = "cito-v-a33-240125"
COLLECTION = "case_data"

MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY")
MISTRAL_API_BASE = os.getenv("MISTRAL_API_BASE", "https://api.mistral.ai/v1")
MISTRAL_MODEL = os.getenv("MISTRAL_MODEL", "mistral-large-latest")
REQUEST_TIMEOUT = int(os.getenv("MISTRAL_TIMEOUT", "60"))
RETRIES = int(os.getenv("MISTRAL_RETRIES", "3"))


# =========================
# Helpers
# =========================
def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def kb_size(s: str) -> float:
    if not s:
        return 0.0
    return len(s.encode("utf-8")) / 1024.0


def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


def require_api_key() -> None:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não definido no ambiente.")


def list_docs(col: Collection) -> List[Dict[str, Any]]:
    return list(
        col.find(
            {"status.pipelineStatus": "caseScraped"},
            projection={
                "_id": 1,
                "identity.stfDecisionId": 1,
                "stfCard.caseUrl": 1,
                "caseData.caseDoctrine": 1,
            },
        ).sort([("_id", 1)])
    )


def print_summary(docs: List[Dict[str, Any]]) -> None:
    print("========================================")
    print("DOCUMENTOS EM caseScraped")
    print("----------------------------------------")
    for d in docs:
        doc_id = d.get("_id")
        stf_id = (d.get("identity") or {}).get("stfDecisionId")
        case_url = (d.get("stfCard") or {}).get("caseUrl")
        doctrine = (d.get("caseData") or {}).get("caseDoctrine") or ""
        size_kb = kb_size(doctrine)
        print(f"_id: {doc_id}")
        print(f"stfDecisionId: {stf_id}")
        print(f"caseUrl: {case_url}")
        print(f"caseDoctrine size: {size_kb:.2f} KB")
        print("----------------------------------------")
    print(f"Total: {len(docs)}")
    print("========================================")


def choose_mode() -> str:
    print("\nEscolha o modo de processamento:")
    print("A - Processar todos")
    print("B - Processar um a um")
    while True:
        opt = input("Opção (A/B): ").strip().upper()
        if opt in {"A", "B"}:
            return opt
        print("Opção inválida. Digite A ou B.")


def build_prompt(text: str) -> str:
    return (
        "Você é um assistente de extração bibliográfica. "
        "Extraia todas as citações bibliográficas presentes no texto a seguir. "
        "Cada citação é uma unidade independente. "
        "Se houver múltiplas referências em uma mesma linha, separe-as. "
        "Não invente dados ausentes. "
        "Retorne EXCLUSIVAMENTE JSON válido no formato:\n"
        "{\"references\":[{"  # noqa: E501
        "\"author\":...,\"publication_title\":...,\"edition\":...,"
        "\"publication_place\":...,\"publisher\":...,\"year\":...,\"page\":...,"
        "\"raw_citation\":...}]}.\n"
        "Campos ausentes devem ser null. "
        "year deve ser inteiro quando identificável, senão null. "
        "Use 'author' como string única com autores separados por ';'.\n\n"
        "Texto:\n"
        f"{text}"
    )


def build_legislation_prompt(text: str) -> str:
    return (
        "PROMPT — EXTRAÇÃO ESTRUTURADA DE REFERÊNCIAS NORMATIVAS\n\n"
        "PAPEL DO MODELO\n"
        "Você é um analisador jurídico especializado em identificação, normalização e extração estruturada de referências normativas a partir de textos jurídicos semiestruturados.\n\n"
        "OBJETIVO\n"
        "Dado um texto de entrada, identificar todas as referências normativas nele contidas e retornar os dados estruturados, normalizados e individualizados, respeitando rigorosamente os padrões descritos abaixo.\n\n"
        "1. ESCOPO DE EXTRAÇÃO\n"
        "1.1 Tipos de normas (nível principal)\n"
        "- Constituição Federal (CF)\n"
        "- Emenda Constitucional (EMC)\n"
        "- Lei Ordinária (LEI)\n"
        "- Lei Complementar (LCP)\n"
        "- Decreto (DEC)\n"
        "- Projeto de Lei (PJL)\n"
        "- Portaria (PRT)\n"
        "- Regimento Interno (RGI)\n"
        "- Normas Internacionais (ex.: CVC)\n"
        "- Outras normas explicitamente identificadas\n\n"
        "1.2 Elementos internos da norma\n"
        "- Artigo (ART)\n"
        "- Inciso (INC)\n"
        "- Parágrafo (PAR)\n"
        "- Letra (LET)\n"
        "- Caput\n"
        "- Parágrafo Único\n"
        "- Anexo / Item (quando aplicável)\n\n"
        "2. REGRAS DE INTERPRETAÇÃO\n"
        "2.1 Identificação de citações\n"
        "- Cada linha pode conter uma ou mais referências.\n"
        "- Quebras de linha, espaços e mudança de tipo (ART → INC → PAR) indicam novas citações.\n"
        "- Citações compostas devem ser tratadas como uma única referência lógica:\n"
        "  - Exemplo: INC-00047 LET-E → Inciso 47, letra E.\n\n"
        "2.2 Herança de contexto\n"
        "- Artigos, incisos e parágrafos herdam a norma principal mais próxima acima.\n"
        "- Se uma norma principal (ex.: LEI, CF, EMC) for declarada, ela se aplica a todas as referências subsequentes até que outra norma principal apareça.\n\n"
        "2.3 Normalização obrigatória\n"
        "- Remover zeros à esquerda de números (ART-00005 → Art. 5).\n"
        "- Padronizar nomes:\n"
        "  - CAPUT → caput\n"
        "  - PAR-ÚNICO → paragrafoUnico\n"
        "- Manter o ano quando explicitado (ANO-YYYY).\n"
        "- Preservar siglas e descrições completas quando fornecidas.\n\n"
        "3. MODELO DE DADOS DE SAÍDA\n"
        "Regra obrigatória de nomenclatura:\n"
        "- Todos os nomes de campos devem estar em inglês, usando camelCase.\n"
        "- Todo o restante do conteúdo (descrições, valores textuais, explicações internas) deve permanecer em português.\n\n"
        "Retorne exclusivamente em JSON, sem comentários adicionais, no formato:\n"
        "{\n"
        "  \"legalNorms\": [\n"
        "    {\n"
        "      \"jurisdictionLevel\": \"federal | estadual | internacional\",\n"
        "      \"normType\": \"CF | EMC | LEI | LCP | DEC | PJL | PRT | RGI | OUTRA\",\n"
        "      \"normIdentifier\": \"CF-1988 | LEI-8112 | EMC-229\",\n"
        "      \"normYear\": 1988,\n"
        "      \"normDescription\": \"descrição completa quando disponível\",\n"
        "      \"normReferences\": [\n"
        "        {\n"
        "          \"articleNumber\": 5,\n"
        "          \"isCaput\": true,\n"
        "          \"incisoNumber\": 3,\n"
        "          \"paragraphNumber\": null,\n"
        "          \"isParagraphSingle\": false,\n"
        "          \"letterCode\": null\n"
        "        }\n"
        "      ]\n"
        "    }\n"
        "  ]\n"
        "}\n\n"
        "Texto:\n"
        f"{text}"
    )


def mistral_chat(prompt: str) -> str:
    url = f"{MISTRAL_API_BASE.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": "Responda apenas com JSON válido."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0,
    }

    last_err = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                raise RuntimeError("Resposta vazia do modelo.")
            print("----- MISTRAL RESPONSE START -----")
            print(content)
            print("----- MISTRAL RESPONSE END -----")
            return content
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(1.5 * attempt)
            else:
                break
    raise RuntimeError(f"Falha ao chamar Mistral API: {last_err}")


def _extract_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        pass

    # remove code fences se existirem
    fenced = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE)
    try:
        return json.loads(fenced)
    except Exception as e:
        raise ValueError(f"JSON inválido retornado pelo modelo: {e}")


def _to_int_year(v: Any) -> Optional[int]:
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if not s:
        return None
    m = re.search(r"\b(1\d{3}|20\d{2})\b", s)
    if not m:
        return None
    return int(m.group(1))


def normalize_references(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    refs = data.get("references")
    if not isinstance(refs, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for r in refs:
        if not isinstance(r, dict):
            continue
        item = {
            "author": r.get("author") or None,
            "publication_title": r.get("publication_title") or None,
            "edition": r.get("edition") or None,
            "publication_place": r.get("publication_place") or None,
            "publisher": r.get("publisher") or None,
            "year": _to_int_year(r.get("year")),
            "page": r.get("page") or None,
            "raw_citation": r.get("raw_citation") or None,
        }
        # mantém apenas se houver raw_citation
        if item["raw_citation"]:
            normalized.append(item)

    return normalized


def normalize_legal_norms(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    norms = data.get("legalNorms")
    if not isinstance(norms, list):
        return []

    cleaned: List[Dict[str, Any]] = []
    for n in norms:
        if not isinstance(n, dict):
            continue
        item = {
            "jurisdictionLevel": n.get("jurisdictionLevel") or None,
            "normType": n.get("normType") or None,
            "normIdentifier": n.get("normIdentifier") or None,
            "normYear": _to_int_year(n.get("normYear")),
            "normDescription": n.get("normDescription") or None,
            "normReferences": n.get("normReferences") if isinstance(n.get("normReferences"), list) else [],
        }
        # Keep only meaningful items
        if item["normIdentifier"] or item["normReferences"]:
            cleaned.append(item)
    return cleaned


def process_doctrine_refs(col: Collection, doc_id, doctrine: str) -> int:
    print("Extração doutrinária: INICIADA")
    if not doctrine.strip():
        print("Extração doutrinária: SEM DADOS (caseDoctrine vazio)")
        return 0

    update_fields: Dict[str, Any] = {
        "processing.caseDoctrineRefsAt": utc_now(),
        "processing.caseDoctrineRefsStatus": "done",
        "processing.caseDoctrineRefsError": None,
    }

    prompt = build_prompt(doctrine)
    raw = mistral_chat(prompt)
    parsed = _extract_json(raw)
    refs = normalize_references(parsed)
    if refs:
        update_fields["caseData.caseDoctrineReferences"] = refs

    col.update_one({"_id": doc_id}, {"$set": update_fields})
    print(f"Extração doutrinária: OK (refs={len(refs)})")
    return len(refs)


def process_legislation_refs(col: Collection, doc_id, legislation: str) -> int:
    print("Extração legislação: INICIADA")
    if not legislation.strip():
        print("Extração legislação: SEM DADOS (caseLegislation vazio)")
        return 0

    prompt = build_legislation_prompt(legislation)
    raw = mistral_chat(prompt)
    parsed = _extract_json(raw)
    leg_refs = normalize_legal_norms(parsed)
    if not leg_refs:
        print("Extração legislação: SEM DADOS (nenhuma referência identificada)")
        return 0

    update_fields = {
        "caseData.caseLegislationReferences": leg_refs,
        "processing.caseLegislationRefsAt": utc_now(),
        "processing.caseLegislationRefsStatus": "done",
        "processing.caseLegislationRefsError": None,
    }
    col.update_one({"_id": doc_id}, {"$set": update_fields})
    print(f"Extração legislação: OK (refs={len(leg_refs)})")
    return len(leg_refs)


def process_doc(col: Collection, doc: Dict[str, Any], interactive: bool) -> None:
    doc_id = doc.get("_id")
    stf_id = (doc.get("identity") or {}).get("stfDecisionId")
    case_url = (doc.get("stfCard") or {}).get("caseUrl")
    doctrine = (doc.get("caseData") or {}).get("caseDoctrine") or ""
    legislation = (doc.get("caseData") or {}).get("caseLegislation") or ""

    if interactive:
        ans = input(f"Processar este documento? (s/n) _id={doc_id}: ").strip().lower()
        if ans != "s":
            return

    doc_refs = process_doctrine_refs(col, doc_id, doctrine)
    leg_count = process_legislation_refs(col, doc_id, legislation)

    if doc_refs == 0 and leg_count == 0:
        print(f"SEM DADOS: {doc_id} (nenhuma referência extraída)")
        return

    print(f"OK: {doc_id} stfDecisionId={stf_id} caseUrl={case_url} refs={doc_refs} leg_refs={leg_count}")


def main() -> int:
    try:
        require_api_key()
    except Exception as e:
        print(str(e))
        return 1

    col = get_collection()
    docs = list_docs(col)
    print_summary(docs)

    if not docs:
        return 0

    mode = choose_mode()
    print(f"Modo escolhido: {'todos' if mode == 'A' else 'um a um'}")

    interactive = mode == "B"
    total = 0
    for doc in docs:
        try:
            process_doc(col, doc, interactive)
            total += 1
        except Exception as e:
            doc_id = doc.get("_id")
            col.update_one(
                {"_id": doc_id},
                {"$set": {
                    "processing.caseDoctrineRefsAt": utc_now(),
                    "processing.caseDoctrineRefsStatus": "error",
                    "processing.caseDoctrineRefsError": str(e),
                }},
            )
            print(f"ERRO: {doc_id} - {e}")

    print(f"Processamento finalizado. Total processados: {total}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
