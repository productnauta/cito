#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
m_extract_doctrine_legislation_groq.py

Processa por processo:
- Extrai doutrinas (rawData.rawDoctrine) -> caseData.caseDoctrineReferences
- Extrai legislações (rawData.rawLegislation) -> caseData.caseLegislationReferences

Filtro inicial: status.pipelineStatus == "enriched"

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

GROQ_API_KEY = "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"  # Substitua por variável de ambiente se preferir
GROQ_MODEL = "llama-3.1-8b-instant"
REQUEST_TIMEOUT = int(os.getenv("GROQ_TIMEOUT", "30"))
RETRIES = int(os.getenv("GROQ_RETRIES", "3"))


# Prompt de sistema para extração de doutrinas (referências jurídicas)
SYSTEM_PROMPT_DOCTRINE = """Act as a Portuguese legal reference extractor. Output ONLY valid JSON following this schema:
{"caseData":{"caseDoctrineReferences":[{"author":str,"publicationTitle":str,"edition":str,"publicationPlace":str,"publisher":str,"year":int,"page":str,"rawCitation":str}]}}

Rules:
1. Segmentation: New item if pattern "SURNAME, Name." appears.
2. Fields: 'edition' as "X ed"; 'year' as 4-digit int; 'page' as string; 'author' only the first one.
3. If data is missing, use null.
4. No conversational filler. No Markdown blocks.
"""

# Prompt do usuário para extração de doutrinas
USER_PROMPT_DOCTRINE = """
Extract the citations from the text below and return ONLY the JSON in the required format.

TEXT:
{doctrine_text}
"""


# Prompt de sistema para extração de legislação (normas jurídicas)
SYSTEM_PROMPT_LEGISLATION = """# SYSTEM — CITO | Legislação → JSON

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
- articleNumber: inteiro de "art./artigo".
- isCaput: true se "caput" OU se apenas "art. X" (sem inciso/parágrafo/alínea).
- incisoNumber: romano → inteiro; ausente = null.
- paragraphNumber: "§ nº" → inteiro; ausente = null.
- isParagraphSingle: true se "parágrafo único".
- letterCode: "alínea a / a)" → "a"; ausente = null.

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

# Prompt do usuário para extração de legislação
USER_PROMPT_LEGISLATION = """# USER MESSAGE — Extração de Legislação

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
        """Retorna a data/hora atual em UTC."""
        return datetime.now(timezone.utc)


def log(msg: str) -> None:
        """Imprime mensagem com timestamp."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"[{ts}] {msg}")


def get_collection() -> Collection:
        """Conecta ao MongoDB e retorna a collection case_data."""
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        return db[COLLECTION]


def require_api_key() -> None:
        """Valida se a chave da API Groq está definida."""
        if not GROQ_API_KEY:
                raise RuntimeError("GROQ_API_KEY não definida.")


def kb_size(s: str) -> float:
        """Calcula o tamanho de uma string em kilobytes."""
        if not s:
                return 0.0
        return len(s.encode("utf-8")) / 1024.0


def groq_chat(system_prompt: str, user_prompt: str) -> str:
        """
        Chama a API Groq com retry automático.
        
        Args:
                system_prompt: Instrução do sistema para o modelo
                user_prompt: Mensagem do usuário/input
        
        Returns:
                Resposta do modelo em texto
                
        Raises:
                RuntimeError: Se falhar após todas as tentativas
        """
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
                                temperature=0,  # Modo determinístico (sem aleatoriedade)
                                timeout=REQUEST_TIMEOUT,
                        )
                        
                        content = response.choices[0].message.content
                        if not content:
                                raise RuntimeError("Resposta vazia do modelo.")
                        return content.strip()
                        
                except Exception as e:
                        last_err = e
                        if attempt < RETRIES:
                                # Aguarda antes de tentar novamente (backoff exponencial)
                                time.sleep(1.5 * attempt)
                        else:
                                break
        raise RuntimeError(f"Falha ao chamar Groq API: {last_err}")


def _extract_json(text: str) -> Dict[str, Any]:
        """
        Extrai JSON da resposta do modelo, tratando múltiplos formatos.
        
        Tenta:
        1. Parse direto
        2. Remover blocos markdown (```)
        3. Truncar até última chave de fechamento válida
        
        Args:
                text: Resposta bruta do modelo
                
        Returns:
                Dicionário Python parseado
                
        Raises:
                RuntimeError: Se nenhum formato válido foi encontrado
        """
        try:
                # Tentativa 1: parse direto
                return json.loads(text)
        except json.JSONDecodeError as e:
                log(f"Erro ao analisar JSON: {e}. Resposta recebida: {text}")
        
        # Tentativa 2: remover blocos fenced (markdown)
        try:
                fenced = re.sub(r"^```(?:json)?|```$", "", text.strip(), flags=re.IGNORECASE | re.MULTILINE)
                return json.loads(fenced)
        except json.JSONDecodeError as e:
                log(f"Erro ao analisar JSON (fenced): {e}. Resposta recebida: {fenced}")
        
        # Tentativa 3: truncar para última chave válida
        try:
                last_brace = fenced.rfind('}')
                if last_brace != -1:
                        partial = fenced[:last_brace + 1]
                        return json.loads(partial)
        except json.JSONDecodeError as e:
                log(f"Erro ao analisar JSON parcial: {e}. Resposta parcial: {partial}")
        
        raise RuntimeError(f"Resposta inválida do modelo: {text}") from e


def _to_int_year(v: Any) -> Optional[int]:
        """
        Converte valor para ano (inteiro 4 dígitos).
        
        Args:
                v: Valor a converter (int, str ou None)
                
        Returns:
                Ano como int ou None se inválido
        """
        if v is None:
                return None
        if isinstance(v, int):
                return v
        s = str(v).strip()
        if not s:
                return None
        # Busca padrão de 4 dígitos (1000-1999 ou 2000-2099)
        m = re.search(r"\b(1\d{3}|20\d{2})\b", s)
        if not m:
                return None
        return int(m.group(1))


def normalize_doctrine_refs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normaliza resposta de extração de doutrinas.
        
        Converte a resposta do modelo para formato padrão, garantindo:
        - Estrutura correta de campos
        - Tipos de dados apropriados (year como int)
        - Valores nulos onde apropriado
        - Filtra itens sem citação bruta (rawCitation)
        
        Args:
                data: Resposta parseada do modelo (pode ter estrutura aninhada)
                
        Returns:
                Lista de referências normalizadas
        """
        refs = None
        # Tentar buscar em estrutura aninhada
        if isinstance(data.get("caseData"), dict):
                refs = data.get("caseData", {}).get("caseDoctrineReferences")
        # Ou em nível raiz
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
                # Só inclui se tem citação bruta
                if item["rawCitation"]:
                        normalized.append(item)
        return normalized


def normalize_legislation_refs(data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Normaliza resposta de extração de legislação.
        
        Args:
                data: Resposta parseada do modelo
                
        Returns:
                Lista de referências legislativas
        """
        refs = None
        # Tentar buscar em nível raiz
        if isinstance(data.get("caseLegislationReferences"), list):
                refs = data.get("caseLegislationReferences")
        # Ou em estrutura aninhada
        elif isinstance(data.get("caseData"), dict):
                refs = data.get("caseData", {}).get("caseLegislationReferences")
        if not isinstance(refs, list):
                return []
        out: List[Dict[str, Any]] = []
        for r in refs:
                if isinstance(r, dict):
                        out.append(r)
        return out


def list_docs(col: Collection) -> List[Dict[str, Any]]:
        """
        Lista documentos com status 'enriched' pronto para extração.
        
        Filtra apenas os campos necessários para reduzir transferência de dados.
        """
        return list(
                col.find(
                        {"status.pipelineStatus": "enriched"},
                        projection={
                                "_id": 1,
                                "caseStfId": 1,
                                "rawData.rawDoctrine": 1,
                                "rawData.rawLegislation": 1,
                        },
                ).sort([("_id", 1)])
        )


def choose_mode() -> bool:
        """
        Solicita ao usuário se deseja confirmar cada item manualmente.
        
        Returns:
                True se deve confirmar item a item, False para processar tudo
        """
        print("1 - Processar todos")
        print("2 - Confirmar item a item")
        opt = input("Escolha uma opção (1/2): ").strip()
        if opt not in {"1", "2"}:
                log("Opção inválida. Encerrando.")
                raise SystemExit(1)
        return opt == "2"


def process_doctrine(col: Collection, doc: Dict[str, Any]) -> int:
        """
        Processa extração de doutrinas para um documento.
        
        Fluxo:
        1. Valida se rawDoctrine existe
        2. Chama API Groq com o texto
        3. Parseia resposta JSON
        4. Normaliza dados
        5. Atualiza MongoDB com resultado ou erro
        
        Args:
                col: Collection MongoDB
                doc: Documento a processar
                
        Returns:
                Número de referências extraídas
        """
        doc_id = doc.get("_id")
        doctrine = (doc.get("rawData") or {}).get("rawDoctrine") or ""

        # Validação: campo vazio
        if not doctrine.strip():
                log(f"IGNORADO: {doc_id} (rawDoctrine vazio)")
                col.update_one(
                        {"_id": doc_id},
                        {"$set": {
                                "processing.caseDoctrineRefsAt": utc_now(),
                                "processing.caseDoctrineRefsStatus": "empty",
                                "processing.caseDoctrineRefsError": "rawDoctrine vazio",
                                "processing.pipelineStatus": "doctrineExtracted",
                                "status.pipelineStatus": "doctrineExtracted",
                        }},
                )
                return 0

        # Chama API
        user_prompt = USER_PROMPT_DOCTRINE.format(doctrine_text=doctrine)
        raw = groq_chat(SYSTEM_PROMPT_DOCTRINE, user_prompt)

        # Exibir resposta integral da API
        print("----- GROQ RESPONSE START -----")
        print(raw)
        print("----- GROQ RESPONSE END -----")

        # Parseia e normaliza
        parsed = _extract_json(raw)
        refs = normalize_doctrine_refs(parsed)

        # Se não extraiu nada
        if not refs:
                log(f"SEM DADOS: {doc_id} (nenhuma referência extraída)")
                col.update_one(
                        {"_id": doc_id},
                        {"$set": {
                                "processing.caseDoctrineRefsAt": utc_now(),
                                "processing.caseDoctrineRefsStatus": "empty",
                                "processing.caseDoctrineRefsError": None,
                                "processing.pipelineStatus": "doctrineExtracted",
                                "status.pipelineStatus": "doctrineExtracted",
                        }},
                )
                return 0

        # Sucesso: armazena no banco
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

        return len(refs)


def process_legislation(col: Collection, doc: Dict[str, Any]) -> int:
        """
        Processa extração de legislação para um documento.
        
        Fluxo análogo a process_doctrine(), mas para normas jurídicas.
        
        Args:
                col: Collection MongoDB
                doc: Documento a processar
                
        Returns:
                Número de referências legislativas extraídas
        """
        doc_id = doc.get("_id")
        legislation = (doc.get("rawData") or {}).get("rawLegislation") or ""

        # Validação: campo vazio
        if not legislation.strip():
                log(f"IGNORADO: {doc_id} (rawLegislation vazio)")
                col.update_one(
                        {"_id": doc_id},
                        {"$set": {
                                "processing.caseLegislationRefsAt": utc_now(),
                                "processing.caseLegislationRefsStatus": "empty",
                                "processing.caseLegislationRefsError": "rawLegislation vazio",
                                "processing.pipelineStatus": "legislationExtracted",
                                "status.pipelineStatus": "legislationExtracted",
                        }},
                )
                return 0

        # Chama API
        user_prompt = USER_PROMPT_LEGISLATION.format(legislation_text=legislation)
        raw = groq_chat(SYSTEM_PROMPT_LEGISLATION, user_prompt)

        # Exibir resposta integral da API
        print("----- GROQ RESPONSE START -----")
        print(raw)
        print("----- GROQ RESPONSE END -----")

        # Parseia e normaliza
        parsed = _extract_json(raw)
        refs = normalize_legislation_refs(parsed)

        # Se não extraiu nada
        if not refs:
                log(f"SEM DADOS: {doc_id} (nenhuma referência legislativa extraída)")
                col.update_one(
                        {"_id": doc_id},
                        {"$set": {
                                "processing.caseLegislationRefsAt": utc_now(),
                                "processing.caseLegislationRefsStatus": "empty",
                                "processing.caseLegislationRefsError": None,
                                "processing.pipelineStatus": "legislationExtracted",
                                "status.pipelineStatus": "legislationExtracted",
                        }},
                )
                return 0

        # Sucesso: armazena no banco
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

        return len(refs)


# Delay entre requisições à API (em segundos) para evitar rate limiting
API_REQUEST_DELAY = 5

def main() -> int:
        """
        Função principal: orquestra o processamento de todos os documentos.
        
        Fluxo:
        1. Valida API key
        2. Conecta ao MongoDB
        3. Lista documentos com status 'enriched'
        4. Exibe resumo
        5. Solicita modo de processamento (automático ou manual)
        6. Para cada documento:
             - Extrai doutrinas via Groq
             - Extrai legislação via Groq
             - Aplica delay entre requisições
             - Atualiza status no MongoDB
        
        Returns:
                0 se sucesso, 1 se erro
        """
        try:
                require_api_key()
        except Exception as e:
                print(str(e))
                return 1

        # Conecta ao MongoDB
        col = get_collection()
        docs = list_docs(col)

        # Exibe resumo dos documentos
        print("-------------------------------------")
        print(f"Documentos com status enriched: {len(docs)}")
        print("-------------------------------------")
        for d in docs:
                doc_id = d.get("_id")
                case_stf_id = d.get("caseStfId")
                doctrine = (d.get("rawData") or {}).get("rawDoctrine") or ""
                legislation = (d.get("rawData") or {}).get("rawLegislation") or ""
                print(
                        f"_id: {doc_id} | caseStfId: {case_stf_id} | "
                        f"rawDoctrine: {kb_size(doctrine):.2f} KB | rawLegislation: {kb_size(legislation):.2f} KB"
                )

        if not docs:
                return 0

        # Solicita modo
        confirm_each = choose_mode()

        total = 0
        api_request_count = 0  # Contador para aplicar delay a cada 2 requisições

        # Processa cada documento
        for doc in docs:
                doc_id = doc.get("_id")
                case_stf_id = doc.get("caseStfId")
                log(f"Iniciando extração de doutrinas e legislações do processo {case_stf_id}...")

                # Modo confirmação: solicita antes de processar
                if confirm_each:
                        ans = input(f"Processar este documento? (s/n) _id={doc_id}: ").strip().lower()
                        if ans != "s":
                                continue

                # Processa doutrinas
                try:
                        doc_refs = process_doctrine(col, doc)
                        api_request_count += 1
                except Exception as e:
                        col.update_one(
                                {"_id": doc_id},
                                {"$set": {
                                        "processing.caseDoctrineRefsAt": utc_now(),
                                        "processing.caseDoctrineRefsStatus": "error",
                                        "processing.caseDoctrineRefsError": str(e),
                                        "processing.pipelineStatus": "error",
                                        "status.pipelineStatus": "error",
                                }},
                        )
                        log(f"ERRO (doutrina): {doc_id} - {e}")
                        doc_refs = 0

                # Processa legislação
                try:
                        leg_refs = process_legislation(col, doc)
                        api_request_count += 1
                except Exception as e:
                        col.update_one(
                                {"_id": doc_id},
                                {"$set": {
                                        "processing.caseLegislationRefsAt": utc_now(),
                                        "processing.caseLegislationRefsStatus": "error",
                                        "processing.caseLegislationRefsError": str(e),
                                        "processing.pipelineStatus": "error",
                                        "status.pipelineStatus": "error",
                                }},
                        )
                        log(f"ERRO (legislação): {doc_id} - {e}")
                        leg_refs = 0

                # Exibe resultado
                print(f"   Total de doutrinas extraídas: {doc_refs}.")
                print(f"   Total de legislações extraídas: {leg_refs}.")
                print(f"   Processamento do item {case_stf_id} finalizado com sucesso.\n")

                total += 1

                # Aplica delay após cada 2 requisições (se não em modo confirmação)
                if not confirm_each and api_request_count >= 2:
                        log(f"Aguardando {API_REQUEST_DELAY} segundos antes de continuar...")
                        time.sleep(API_REQUEST_DELAY)
                        api_request_count = 0

                # Modo confirmação: solicita se continua
                if confirm_each:
                        cont = input("Processar próximo item? (s/n): ").strip().lower()
                        if cont != "s":
                                break

        log(f"Processamento finalizado. Total processados: {total}")
        return 0


if __name__ == "__main__":
        raise SystemExit(main())