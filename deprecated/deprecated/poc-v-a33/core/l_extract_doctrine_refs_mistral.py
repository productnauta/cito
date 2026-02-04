#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
l_extract_doctrine_refs_mistral.py

Extrai referências doutrinárias via Mistral API a partir de rawData.rawDoctrine
em case_data, com status.pipelineStatus == "htmlFetched".

Dependências:
  pip install pymongo requests
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

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


SYSTEM_PROMPT = """# SYSTEM PROMPT — Extração de Doutrina (Mistral, token-efficient)

Você é um **extrator de referências doutrinárias jurídicas** em português (padrão ABNT aproximado).

## Tarefa
Identificar **cada citação individual** em um texto e extrair dados estruturados para `caseData.caseDoctrineReferences`.

## Segmentação
- Uma citação pode estar:
  - em uma linha; ou
  - colada a outra na mesma linha.
- **Não use vírgulas** como separador de citações.
- Nova citação normalmente inicia por `SOBRENOME, Nome.`.
- Após `ano.` ou `p. ...`, se surgir novo padrão `SOBRENOME, Nome.`, iniciar nova citação.

## Regras
- Não inventar dados.
- Campos ausentes → `null`.
- `year`: inteiro (4 dígitos) ou `null`.
- `edition`: normalizar para `"X ed"`.
- `page`: string (ex.: `"181"`, `"233-234"`, `"233-234 e 1.561"`).
- Múltiplos autores: usar **apenas o primeiro** em `author`.
- `rawCitation`: citação completa, preservando o texto original.

## Campos por item
- `author`
- `publicationTitle`
- `edition`
- `publicationPlace`
- `publisher`
- `year`
- `page`
- `rawCitation`

## Exemplo de identificação (ilustrativo)

### Entrada
ALEXY, Robert. Teoria dos direitos fundamentais. 2. ed. Trad. Virgílio Afonso da Silva. São Paulo: Malheiros, 2015, p. 582.  
CANOTILHO, José Joaquim Gomes. Direito constitucional. 6. ed. Coimbra: Almedina, 1993, p. 139.

### Saída esperada (estrutura)
{
  "caseData": {
    "caseDoctrineReferences": [
      {
        "author": "ALEXY, Robert",
        "publicationTitle": "Teoria dos direitos fundamentais",
        "edition": "2 ed",
        "publicationPlace": "São Paulo",
        "publisher": "Malheiros",
        "year": 2015,
        "page": "582",
        "rawCitation": "ALEXY, Robert. Teoria dos direitos fundamentais. 2. ed. Trad. Virgílio Afonso da Silva. São Paulo: Malheiros, 2015, p. 582."
      },
      {
        "author": "CANOTILHO, José Joaquim Gomes",
        "publicationTitle": "Direito constitucional",
        "edition": "6 ed",
        "publicationPlace": "Coimbra",
        "publisher": "Almedina",
        "year": 1993,
        "page": "139",
        "rawCitation": "CANOTILHO, José Joaquim Gomes. Direito constitucional. 6. ed. Coimbra: Almedina, 1993, p. 139."
      }
    ]
  }
}
"""

USER_PROMPT_TEMPLATE = """# USER MESSAGE — Extração de Doutrina

Extraia as referências doutrinárias do texto abaixo e retorne **apenas JSON válido**, conforme definido no SYSTEM PROMPT.

{doctrine_text}

## Saída obrigatória
Retornar **somente JSON válido**, exatamente na estrutura acima.  
Não incluir markdown, comentários ou texto adicional.
"""


LEGISLATION_SYSTEM_PROMPT = """# SYSTEM — CITO | Legislação → JSON

Tarefa: extrair referências legislativas de texto jurídico (PT-BR) e retornar SOMENTE JSON válido.

SAÍDA ÚNICA
{
  "caseLegislationReferences": [
    {
      "jurisdictionLevel": "federal|state|municipal|unknown",
      "normType": "CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA",
      "normIdentifier": "string",
      "normYear": 0,
      "normDescription": "string",
      "normReferences": [
        {
          "articleNumber": 0,
          "isCaput": true,
          "incisoNumber": 0,
          "paragraphNumber": 0,
          "isParagraphSingle": false,
          "letterCode": "a"
        }
      ]
    }
  ]
}

REGRAS
- Responder apenas com JSON (sem markdown/texto).
- Agrupar por norma; deduplicar normas e dispositivos.
- Permitir múltiplas normas e dispositivos.

NORMALIZAÇÃO
- articleNumber: inteiro de “art./artigo”.
- isCaput: true se “caput” OU se apenas “art. X” (sem inciso/parágrafo/alínea).
- incisoNumber: romano → inteiro; ausente = null.
- paragraphNumber: “§ nº” → inteiro; ausente = null.
- isParagraphSingle: true se “parágrafo único”.
- letterCode: “alínea a / a)” → “a”; ausente = null.

NORMA
- normIdentifier: CF-1988; EC-n-ano; LC-n-ano; LEI-n-ano; DECRETO-n-ano (remover ponto do número).
- normYear: inteiro; ausente = 0.
- normDescription: nome curto se explícito; senão "".
- jurisdictionLevel: inferir; senão "unknown".

CASO-LIMITE
- Norma sem dispositivo explícito: normReferences com um item:
  {"articleNumber": null, "isCaput": false, "incisoNumber": null, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null}

EXEMPLO
Texto: "CF/88, art. 5º, caput, inc. III; Lei 8.112/1990 (RJU), art. 1º, parágrafo único, alínea a."
Saída:
{
  "caseLegislationReferences": [
    {
      "jurisdictionLevel": "federal",
      "normType": "CF",
      "normIdentifier": "CF-1988",
      "normYear": 1988,
      "normDescription": "Constituição Federal",
      "normReferences": [
        {"articleNumber": 5, "isCaput": true, "incisoNumber": 3, "paragraphNumber": null, "isParagraphSingle": false, "letterCode": null}
      ]
    },
    {
      "jurisdictionLevel": "federal",
      "normType": "LEI",
      "normIdentifier": "LEI-8112-1990",
      "normYear": 1990,
      "normDescription": "Regime Jurídico Único",
      "normReferences": [
        {"articleNumber": 1, "isCaput": false, "incisoNumber": null, "paragraphNumber": null, "isParagraphSingle": true, "letterCode": "a"}
      ]
    }
  ]
}

VALIDAÇÃO
- JSON parseável; usar null (não strings).
"""

LEGISLATION_USER_PROMPT_TEMPLATE = """# USER MESSAGE — Extração de Legislação

Extraia as referências legislativas do texto abaixo e retorne **apenas JSON válido**, conforme definido no SYSTEM PROMPT.

{legislation_text}

## Saída obrigatória
Retornar **somente JSON válido**, exatamente na estrutura acima.  
Não incluir markdown, comentários ou texto adicional.
"""


# =========================
# Helpers
# =========================

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}")


def get_collection() -> Collection:
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]
    return db[COLLECTION]


def require_api_key() -> None:
    if not MISTRAL_API_KEY:
        raise RuntimeError("MISTRAL_API_KEY não definido no ambiente.")


def kb_size(s: str) -> float:
    if not s:
        return 0.0
    return len(s.encode("utf-8")) / 1024.0


def mistral_chat(system_prompt: str, user_prompt: str) -> str:
    url = f"{MISTRAL_API_BASE.rstrip('/')}/chat/completions"
    headers = {
        "Authorization": f"Bearer {MISTRAL_API_KEY}",
        "Content-Type": "application/json",
    }
    payload = {
        "model": MISTRAL_MODEL,
        "messages": [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
        "temperature": 0,
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            resp = requests.post(url, headers=headers, json=payload, timeout=REQUEST_TIMEOUT)
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            data = resp.json()
            content = data.get("choices", [{}])[0].get("message", {}).get("content", "")
            if not content:
                raise RuntimeError("Resposta vazia do modelo.")
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
    return json.loads(fenced)


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
    # aceita dois formatos: {caseData:{caseDoctrineReferences:[...]}} ou {caseDoctrineReferences:[...]}
    refs = None
    if isinstance(data.get("caseData"), dict):
        refs = data.get("caseData", {}).get("caseDoctrineReferences")
    if refs is None:
        refs = data.get("caseDoctrineReferences")

    if not isinstance(refs, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for r in refs:
        if not isinstance(r, dict):
            continue
        item = {
            "author": r.get("author") or None,
            "publicationTitle": r.get("publicationTitle") or None,
            "edition": r.get("edition") or None,
            "publicationPlace": r.get("publicationPlace") or None,
            "publisher": r.get("publisher") or None,
            "year": _to_int_year(r.get("year")),
            "page": r.get("page") or None,
            "rawCitation": r.get("rawCitation") or None,
        }
        if item["rawCitation"]:
            normalized.append(item)
    return normalized


def normalize_legislation_references(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    refs = None
    if isinstance(data.get("caseLegislationReferences"), list):
        refs = data.get("caseLegislationReferences")
    elif isinstance(data.get("caseData"), dict):
        refs = data.get("caseData", {}).get("caseLegislationReferences")
    if not isinstance(refs, list):
        return []
    cleaned: List[Dict[str, Any]] = []
    for r in refs:
        if isinstance(r, dict):
            cleaned.append(r)
    return cleaned


def list_docs(col: Collection) -> List[Dict[str, Any]]:
    return list(
        col.find(
            {"status.pipelineStatus": "enriched"},
            projection={
                "_id": 1,
                "caseStfId": 1,
                "caseIdentification.caseUrl": 1,
                "rawData.rawDoctrine": 1,
            },
        ).sort([("_id", 1)])
    )


def process_doc(col: Collection, doc: Dict[str, Any], confirm_each: bool) -> None:
    doc_id = doc.get("_id")
    case_stf_id = doc.get("caseStfId")
    case_url = (doc.get("caseIdentification") or {}).get("caseUrl")
    doctrine = (doc.get("rawData") or {}).get("rawDoctrine") or ""

    if confirm_each:
        ans = input(f"Processar este documento? (s/n) _id={doc_id}: ").strip().lower()
        if ans != "s":
            return

    if not doctrine.strip():
        log(f"IGNORADO: {doc_id} (rawDoctrine vazio)")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseDoctrineRefsAt": utc_now(),
                "processing.caseDoctrineRefsStatus": "empty",
                "processing.caseDoctrineRefsError": "rawDoctrine vazio",
            }},
        )
        return

    user_prompt = USER_PROMPT_TEMPLATE.format(doctrine_text=doctrine)
    raw = mistral_chat(SYSTEM_PROMPT, user_prompt)
    parsed = _extract_json(raw)
    refs = normalize_references(parsed)

    if not refs:
        log(f"SEM DADOS: {doc_id} (nenhuma referência extraída)")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseDoctrineRefsAt": utc_now(),
                "processing.caseDoctrineRefsStatus": "empty",
                "processing.caseDoctrineRefsError": None,
            }},
        )
        return

    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "caseData.caseDoctrineReferences": refs,
            "processing.caseDoctrineRefsAt": utc_now(),
            "processing.caseDoctrineRefsStatus": "done",
            "processing.caseDoctrineRefsError": None,
            "processing.pipelineStatus": "doctrineExtracted",
            "status.pipelineStatus": "doctrineExtracted",
        }},
    )

    log(f"OK: {doc_id} caseStfId={case_stf_id} caseUrl={case_url} refs={len(refs)}")


def list_legislation_docs(col: Collection) -> List[Dict[str, Any]]:
    return list(
        col.find(
            {"status.pipelineStatus": "doctrineExtracted"},
            projection={
                "_id": 1,
                "caseStfId": 1,
                "caseIdentification.caseUrl": 1,
                "rawData.rawLegislation": 1,
            },
        ).sort([("_id", 1)])
    )


def process_legislation_doc(col: Collection, doc: Dict[str, Any], confirm_each: bool) -> None:
    doc_id = doc.get("_id")
    case_stf_id = doc.get("caseStfId")
    case_url = (doc.get("caseIdentification") or {}).get("caseUrl")
    legislation = (doc.get("rawData") or {}).get("rawLegislation") or ""

    if confirm_each:
        ans = input(f"Processar este documento? (s/n) _id={doc_id}: ").strip().lower()
        if ans != "s":
            return

    if not legislation.strip():
        log(f"IGNORADO: {doc_id} (rawLegislation vazio)")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseLegislationRefsAt": utc_now(),
                "processing.caseLegislationRefsStatus": "empty",
                "processing.caseLegislationRefsError": "rawLegislation vazio",
            }},
        )
        return

    user_prompt = LEGISLATION_USER_PROMPT_TEMPLATE.format(legislation_text=legislation)
    raw = mistral_chat(LEGISLATION_SYSTEM_PROMPT, user_prompt)

    # Exibir resposta integral da API para conferência
    print("----- MISTRAL RESPONSE START -----")
    print(raw)
    print("----- MISTRAL RESPONSE END -----")

    parsed = _extract_json(raw)
    refs = normalize_legislation_references(parsed)

    if not refs:
        log(f"SEM DADOS: {doc_id} (nenhuma referência legislativa extraída)")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseLegislationRefsAt": utc_now(),
                "processing.caseLegislationRefsStatus": "empty",
                "processing.caseLegislationRefsError": None,
            }},
        )
        return

    col.update_one(
        {"_id": doc_id},
        {"$set": {
            "caseData.caseLegislationReferences": refs,
            "processing.caseLegislationRefsAt": utc_now(),
            "processing.caseLegislationRefsStatus": "done",
            "processing.caseLegislationRefsError": None,
            "processing.pipelineStatus": "legislationExtracted",
            "status.pipelineStatus": "legislationExtracted",
        }},
    )

    log(f"OK: {doc_id} caseStfId={case_stf_id} caseUrl={case_url} legRefs={len(refs)}")


def main() -> int:
    try:
        require_api_key()
    except Exception as e:
        print(str(e))
        return 1

    col = get_collection()
    docs = list_docs(col)

    print("-------------------------------------")
    print(f"Documentos com status enriched: {len(docs)}")
    print("-------------------------------------")

    for d in docs:
        doc_id = d.get("_id")
        case_stf_id = d.get("caseStfId")
        doctrine = (d.get("rawData") or {}).get("rawDoctrine") or ""
        print(f"_id: {doc_id} | caseStfId: {case_stf_id} | rawDoctrine: {kb_size(doctrine):.2f} KB")

    if not docs:
        return 0

    print("\n1 - Processar todos")
    print("2 - Confirmar item a item")
    opt = input("Escolha uma opção (1/2): ").strip()
    if opt not in {"1", "2"}:
        log("Opção inválida. Encerrando.")
        return 1

    confirm_each = opt == "2"

    total = 0
    for doc in docs:
        try:
            process_doc(col, doc, confirm_each)
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
            log(f"ERRO: {doc_id} - {e}")

    log(f"Processamento finalizado. Total processados: {total}")

    # Etapa 2: legislação
    leg_docs = list_legislation_docs(col)
    print("\n-------------------------------------")
    print(f"Documentos com status doctrineExtracted: {len(leg_docs)}")
    print("-------------------------------------")
    for d in leg_docs:
        doc_id = d.get("_id")
        case_stf_id = d.get("caseStfId")
        legislation = (d.get("rawData") or {}).get("rawLegislation") or ""
        print(f"_id: {doc_id} | caseStfId: {case_stf_id} | rawLegislation: {kb_size(legislation):.2f} KB")

    if not leg_docs:
        return 0

    print("\n1 - Processar todos")
    print("2 - Confirmar item a item")
    opt = input("Escolha uma opção (1/2): ").strip()
    if opt not in {"1", "2"}:
        log("Opção inválida. Encerrando.")
        return 1

    confirm_each = opt == "2"

    total_leg = 0
    for doc in leg_docs:
        try:
            process_legislation_doc(col, doc, confirm_each)
            total_leg += 1
        except Exception as e:
            doc_id = doc.get("_id")
            col.update_one(
                {"_id": doc_id},
                {"$set": {
                    "processing.caseLegislationRefsAt": utc_now(),
                    "processing.caseLegislationRefsStatus": "error",
                    "processing.caseLegislationRefsError": str(e),
                }},
            )
            log(f"ERRO: {doc_id} - {e}")

    log(f"Processamento legislação finalizado. Total processados: {total_leg}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
