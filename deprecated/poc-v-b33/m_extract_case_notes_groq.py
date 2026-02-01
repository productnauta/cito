#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m_extract_case_notes_groq.py

Processa por processo:
- Extrai citações de acórdãos, decisões monocráticas, legislações estrangeiras, etc.
- Filtro inicial: status.pipelineStatus == "legislationExtracted"

Dependências:
  pip install pymongo groq
"""

from __future__ import annotations

import json
import os
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from groq import Groq
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

GROQ_API_KEY = "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"
GROQ_MODEL = "llama-3.1-8b-instant"
REQUEST_TIMEOUT = int(os.getenv("GROQ_TIMEOUT", "60"))
RETRIES = int(os.getenv("GROQ_RETRIES", "3"))
API_DELAY_SECONDS = 20  # Delay configurável entre processamentos


# =========================
# Prompts
# =========================
SYSTEM_PROMPT_NOTES = """
Act as a legal JSON extractor. Output ONLY valid JSON. 
Schema: {"caseData":{"caseNotes":[{"noteType":str,"descriptors":[str],"rawLine":str,"items":[{"itemType":str,"caseClass":str|null,"caseNumber":str|null,"suffix":str|null,"orgTag":str|null,"rawRef":str}]}]}}

Rules:
1. noteType: "Acórdão"->stf_acordao; "Monocrática"->stf_monocratica; "Outros"->outros_tribunais; "Estrangeira"->decisao_estrangeira/legislacao_estrangeira; "Veja"->veja.
2. Override: If "Carta/Recomendação/Plano/ONU/CNJ" -> itemType: treaty_or_recommendation.
3. Descriptors: Extract from (X, Y). Apply to all following lines until next header or descriptor block.
4. STF Parsing: caseClass (ADI/RE/HC/etc), caseNumber (digits), suffix (AgR/MC/RG/ED), orgTag (TP/1ªT/2ªT).
5. Integrity: Use null for missing data. Keep original text in rawLine/rawRef.
"""

USER_PROMPT_NOTES = """
Extract legal citations from the text below and return ONLY valid JSON as per the SYSTEM PROMPT:

{notes_text}
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
    if not GROQ_API_KEY:
        raise RuntimeError("GROQ_API_KEY não definida.")


def kb_size(s: str) -> float:
    if not s:
        return 0.0
    return len(s.encode("utf-8")) / 1024.0


def groq_chat(system_prompt: str, user_prompt: str) -> str:
    client = Groq(api_key=GROQ_API_KEY)
    
    last_err: Optional[Exception] = None
    for attempt in range(1, RETRIES + 1):
        try:
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
                timeout=REQUEST_TIMEOUT,
            )
            
            content = response.choices[0].message.content
            if not content:
                raise RuntimeError("Resposta vazia do modelo.")
            return content.strip()
            
        except Exception as e:
            last_err = e
            if attempt < RETRIES:
                time.sleep(1.5 * attempt)
            else:
                break
    raise RuntimeError(f"Falha ao chamar Groq API: {last_err}")


def _extract_json(text: str) -> Dict[str, Any]:
    try:
        return json.loads(text)
    except Exception:
        pass
    fenced = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE)
    return json.loads(fenced)


def normalize_notes_refs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Normaliza as referências de notas extraídas"""
    notes = None
    if isinstance(data.get("caseData"), dict):
        notes = data.get("caseData", {}).get("caseNotes")
    if notes is None:
        notes = data.get("caseNotes")
    if not isinstance(notes, list):
        return []

    normalized: List[Dict[str, Any]] = []
    for note in notes:
        if not isinstance(note, dict):
            continue
            
        # Normalizar noteType
        note_type_map = {
            "Acórdão": "stf_acordao",
            "Monocrática": "stf_monocratica", 
            "Outros": "outros_tribunais",
            "Estrangeira": "decisao_estrangeira",
            "Veja": "veja"
        }
        
        note_type = note.get("noteType")
        if isinstance(note_type, str) and note_type in note_type_map:
            note_type = note_type_map[note_type]
        
        # Normalizar descriptors
        descriptors = note.get("descriptors")
        if not isinstance(descriptors, list):
            descriptors = []
        else:
            descriptors = [str(d).strip() for d in descriptors if d]
        
        # Normalizar items
        items = note.get("items", [])
        if not isinstance(items, list):
            items = []
        
        normalized_items = []
        for item in items:
            if not isinstance(item, dict):
                continue
                
            # Aplicar override para treaty_or_recommendation
            raw_ref = item.get("rawRef", "")
            item_type = item.get("itemType", "")
            
            if isinstance(raw_ref, str) and any(word in raw_ref for word in 
                                               ["Carta", "Recomendação", "Plano", "ONU", "CNJ"]):
                item_type = "treaty_or_recommendation"
            
            normalized_item = {
                "itemType": str(item_type) if item_type else None,
                "caseClass": item.get("caseClass") if item.get("caseClass") else None,
                "caseNumber": str(item.get("caseNumber")) if item.get("caseNumber") else None,
                "suffix": item.get("suffix") if item.get("suffix") else None,
                "orgTag": item.get("orgTag") if item.get("orgTag") else None,
                "rawRef": str(raw_ref) if raw_ref else None
            }
            normalized_items.append(normalized_item)
        
        normalized_note = {
            "noteType": note_type,
            "descriptors": descriptors,
            "rawLine": note.get("rawLine", ""),
            "items": normalized_items
        }
        normalized.append(normalized_note)
    
    return normalized


def list_docs(col: Collection) -> List[Dict[str, Any]]:
    """Lista documentos com status 'legislationExtracted'"""
    return list(
        col.find(
            {"status.pipelineStatus": "legislationExtracted"},
            projection={
                "_id": 1,
                "caseStfId": 1,
                "rawData.rawNotes": 1,
                "processing.pipelineStatus": 1
            },
        ).sort([("_id", 1)])
    )


def choose_mode() -> bool:
    """Pergunta ao usuário o modo de processamento"""
    print("\n" + "="*50)
    print("MODO DE PROCESSAMENTO")
    print("="*50)
    print("1 - Processar todos os documentos automaticamente")
    print("2 - Confirmar cada documento individualmente")
    print("="*50)
    
    while True:
        opt = input("\nEscolha uma opção (1/2): ").strip()
        if opt == "1":
            return False  # Não confirmar individualmente
        elif opt == "2":
            return True   # Confirmar individualmente
        else:
            print("Opção inválida. Digite 1 ou 2.")


def process_notes(col: Collection, doc: Dict[str, Any]) -> int:
    """Processa as notas de um documento"""
    doc_id = doc.get("_id")
    case_stf_id = doc.get("caseStfId", "N/A")
    
    # Obter conteúdo das notas
    notes_content = (doc.get("rawData") or {}).get("rawNotes") or ""
    
    if not notes_content.strip():
        log(f"IGNORADO: {case_stf_id} (rawNotes vazio)")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseNotesExtractedAt": utc_now(),
                "processing.caseNotesExtractedStatus": "empty",
                "processing.caseNotesExtractedError": "rawNotes vazio",
                "status.pipelineStatus": "notesExtracted",
            }},
        )
        return 0
    
    # Preparar prompt para API
    user_prompt = USER_PROMPT_NOTES.format(notes_text=notes_content)
    
    try:
        # Chamar API Groq
        raw_response = groq_chat(SYSTEM_PROMPT_NOTES, user_prompt)
        
        # Validar e parsear JSON
        parsed = _extract_json(raw_response)
        notes_refs = normalize_notes_refs(parsed)
        
        # Contar total de citações
        total_citations = sum(len(note.get("items", [])) for note in notes_refs)
        
        # Atualizar documento no MongoDB
        update_data = {
            "caseData.caseNotes": notes_refs,
            "processing.caseNotesExtractedAt": utc_now(),
            "processing.caseNotesExtractedStatus": "done",
            "processing.caseNotesExtractedError": None,
            "status.pipelineStatus": "notesExtracted",
        }
        
        col.update_one(
            {"_id": doc_id},
            {"$set": update_data},
        )
        
        return total_citations
        
    except Exception as e:
        log(f"ERRO ao processar notas: {case_stf_id} - {e}")
        col.update_one(
            {"_id": doc_id},
            {"$set": {
                "processing.caseNotesExtractedAt": utc_now(),
                "processing.caseNotesExtractedStatus": "error",
                "processing.caseNotesExtractedError": str(e),
                "status.pipelineStatus": "error",
            }},
        )
        return 0


def display_document_info(doc: Dict[str, Any]) -> None:
    """Exibe informações do documento"""
    doc_id = doc.get("_id")
    case_stf_id = doc.get("caseStfId", "N/A")
    notes = (doc.get("rawData") or {}).get("rawNotes") or ""
    
    print(f"\n{'='*60}")
    print(f"DOCUMENTO: {case_stf_id}")
    print(f"ID: {doc_id}")
    print(f"Tamanho das notas: {kb_size(notes):.2f} KB")
    print(f"{'='*60}")


def main() -> int:
    """Função principal"""
    try:
        require_api_key()
    except Exception as e:
        print(f"ERRO: {e}")
        return 1
    
    # Conectar ao MongoDB
    col = get_collection()
    
    # Listar documentos para processamento
    docs = list_docs(col)
    
    # Exibir quantidade de documentos encontrados
    print("\n" + "="*60)
    print(f"DOCUMENTOS ENCONTRADOS PARA PROCESSAMENTO: {len(docs)}")
    print("="*60)
    
    if not docs:
        print("Nenhum documento com status 'legislationExtracted' encontrado.")
        return 0
    
    # Exibir lista de documentos
    print("\nLista de documentos para processamento:")
    print("-"*60)
    for i, doc in enumerate(docs, 1):
        case_stf_id = doc.get("caseStfId", "N/A")
        notes = (doc.get("rawData") or {}).get("rawNotes") or ""
        print(f"{i:3d}. {case_stf_id:20s} | Notas: {kb_size(notes):6.2f} KB")
    
    # Perguntar modo de processamento
    confirm_each = choose_mode()
    
    # Processar documentos
    total_processed = 0
    total_citations = 0
    
    print("\n" + "="*60)
    print("INICIANDO PROCESSAMENTO")
    print("="*60)
    
    for i, doc in enumerate(docs, 1):
        doc_id = doc.get("_id")
        case_stf_id = doc.get("caseStfId", "N/A")
        
        # Exibir informações do documento atual
        print(f"\n[{i}/{len(docs)}] Processando documento {case_stf_id}...")
        
        # Se modo de confirmação individual, perguntar ao usuário
        if confirm_each:
            display_document_info(doc)
            resposta = input("\nProcessar este documento? (s/n): ").strip().lower()
            if resposta != 's':
                print("Documento pulado.")
                continue
        
        # Processar notas do documento
        try:
            citations_count = process_notes(col, doc)
            total_citations += citations_count
            total_processed += 1
            
            # Exibir resultados do processamento
            print(f"   Total de citações extraídas: {citations_count}.")
            print(f"   Processamento do item {case_stf_id} finalizado com sucesso.")
            
            # Aplicar delay entre processamentos (exceto no último)
            if i < len(docs) and not confirm_each:
                print(f"\nAguardando {API_DELAY_SECONDS} segundos antes do próximo...")
                time.sleep(API_DELAY_SECONDS)
                
        except Exception as e:
            print(f"   ERRO no processamento: {e}")
            continue
    
    # Exibir resumo final
    print("\n" + "="*60)
    print("RESUMO DO PROCESSAMENTO")
    print("="*60)
    print(f"Total de documentos processados: {total_processed}")
    print(f"Total de citações extraídas: {total_citations}")
    print(f"Status final: {'COMPLETO' if total_processed > 0 else 'NENHUM PROCESSADO'}")
    print("="*60)
    
    return 0


if __name__ == "__main__":
    raise SystemExit(main())