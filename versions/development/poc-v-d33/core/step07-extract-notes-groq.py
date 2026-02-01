#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step07-extract-notes-groq.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts references from notes via Groq and stores structured notes references.
Inputs: config/mongo.json, config/ai-model.json, caseContent.md.notes.
Outputs: caseData.notesReferences + processing/status updates.
Pipeline: chunk text -> Groq extraction (retry/repair) -> persist notes references.
Dependencies: pymongo groq
------------------------------------------------------------

"""

from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from groq import Groq
from pymongo import MongoClient
from pymongo.collection import Collection


# =============================================================================
# 0) LOG / TEMPO
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def log(level: str, msg: str) -> None:
    print(f"[{_ts()}] [{level}] {msg}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
AI_MODEL_CONFIG_PATH = CONFIG_DIR / "ai-model.json"

CASE_DATA_COLLECTION = "case_data"


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


@dataclass(frozen=True)
class GroqCfg:
    api_key: str
    model: str
    request_timeout_seconds: int
    retries: int
    api_delay_seconds: float


def load_json(path: Path) -> Dict[str, Any]:
    """Carrega um JSON do disco com validação mínima."""
    if not path.exists():
        raise FileNotFoundError(f"Config não encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    """Interpreta config/mongo.json."""
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config inválida: chave 'mongo' ausente ou inválida.")
    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri:
        raise ValueError("Config inválida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config inválida: 'mongo.database' vazio.")
    return MongoCfg(uri=uri, database=db)


def build_groq_cfg(raw: Dict[str, Any]) -> GroqCfg:
    """Interpreta config/ai-model.json."""
    g = raw.get("groq")
    if not isinstance(g, dict):
        raise ValueError("Config inválida: provider 'groq' ausente em ai-model.json.")
    api_key = str(g.get("api_key") or "").strip()
    model = str(g.get("model") or "").strip()
    if not api_key:
        raise ValueError("Config inválida: 'groq.api_key' vazio.")
    if not model:
        raise ValueError("Config inválida: 'groq.model' vazio.")
    return GroqCfg(
        api_key=api_key,
        model=model,
        request_timeout_seconds=int(g.get("request_timeout_seconds") or 60),
        retries=int(g.get("retries") or 1),
        api_delay_seconds=float(g.get("api_delay_seconds") or 0),
    )


def get_case_data_collection() -> Collection:
    """Cria conexão com MongoDB e retorna collection case_data."""
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) PROMPT (EXTRAÇÃO DE REFERÊNCIAS EM NOTAS)
# =============================================================================

SYSTEM_PROMPT = """# ROLE
Você é um extrator de dados jurídicos especializado em tribunais superiores e direito comparado. Sua tarefa é converter citações textuais em um JSON estruturado e rigoroso.

# OUTPUT RULES
- Retorne APENAS o objeto JSON.
- Não inclua explicações ou blocos de Markdown fora do JSON.
- Se um campo for inexistente, use `null`.
- Mantenha a grafia original em `rawLine` e `rawRef`.

# EXTRACTION LOGIC & MAPPING
1. **Categorização (noteType):**
   - "Acórdãos citados" -> `stf_acordao`
   - "Decisão monocrática" -> `stf_monocratica`
   - "Decisões de outros tribunais" -> `outros_tribunais`
   - "Legislação estrangeira" -> `legislacao_estrangeira`
   - "Decisões estrangeiras" -> `decisao_estrangeira`
   - Referências iniciadas por "Veja" ou "Cf." -> `veja`

2. **Parsing de Itens (Brasil):**
   - **caseClass:** Sigla da classe (ex: ADI, RE, HC, ADPF).
   - **caseNumber:** Apenas os dígitos numéricos.
   - **suffix:** Siglas acessórias (ex: MC, QO, AgR, ED, RG).
   - **orgTag:** Órgão julgador, se houver (ex: TP, 1ªT).

3. **Tratamento Especial:**
   - Documentos da ONU, Tratados, Planos ou Recomendações do CNJ -> `itemType: "treaty_or_recommendation"`.
   - Citações de Revistas (ex: RTJ 63/299) -> `itemType: "legal_journal"`.

# JSON SCHEMA
{
  "caseData": {
    "notesReferences": [
      {
        "noteType": "string",
        "rawLine": "string",
        "items": [
          {
            "itemType": "decision | legislation | treaty_or_recommendation | legal_journal",
            "caseClass": "string | null",
            "caseNumber": "string | null",
            "suffix": "string | null",
            "orgTag": "string | null",
            "country": "string | null",
            "rawRef": "string"
          }
        ]
      }
    ]
  }
}
"""

# Prompt mais rígido para retry quando houver erro de parsing/JSON inválido.
STRICT_SYSTEM_PROMPT = SYSTEM_PROMPT + (
    "\n# OUTPUT STRICT\n"
    "- Responda APENAS com JSON válido.\n"
    "- Não use Markdown, não use texto fora do JSON.\n"
    "- Certifique-se de que todas as vírgulas e aspas estejam corretas.\n"
)

# Prompt ainda mais rígido: JSON estrito e minificado.
STRICT_MINIFIED_SYSTEM_PROMPT = STRICT_SYSTEM_PROMPT + (
    "- Retorne o JSON em uma única linha (minificado).\n"
)


def build_user_prompt(notes_text: str) -> str:
    """Prompt do usuário com instrução de segmentação (itens distintos)."""
    return (
        "Extraia as citações do texto abaixo seguindo o SCHEMA definido. \n"
        'Atenção: Separe cada citação individualmente (ex: "ADPF 54 MC" e "ADPF 54 QO" são dois itens distintos).\n\n'
        "TEXTO PARA ANÁLISE:\n"
        f"{notes_text}"
    )


def build_strict_user_prompt(notes_text: str) -> str:
    """Prompt do usuário para retry, reforçando JSON válido."""
    return (
        "RETORNE APENAS JSON VÁLIDO. NÃO inclua Markdown ou qualquer texto extra.\n\n"
        + build_user_prompt(notes_text)
    )


def build_minified_user_prompt(notes_text: str) -> str:
    """Prompt do usuário para retry com JSON minificado."""
    return (
        "RETORNE APENAS JSON VÁLIDO EM UMA ÚNICA LINHA (MINIFICADO).\n\n"
        + build_user_prompt(notes_text)
    )


# =============================================================================
# 3) GROQ CALL
# =============================================================================

def call_groq(
    cfg: GroqCfg,
    notes_text: str,
    *,
    system_prompt: Optional[str] = None,
    user_prompt: Optional[str] = None,
) -> Any:
    """Chamada à Groq com retry/backoff simples e timeout."""
    client = Groq(api_key=cfg.api_key)

    system_prompt = system_prompt or SYSTEM_PROMPT
    user_prompt = user_prompt or build_user_prompt(notes_text)

    last_err: Optional[Exception] = None
    for attempt in range(1, cfg.retries + 1):
        if cfg.api_delay_seconds and attempt > 1:
            time.sleep(cfg.api_delay_seconds)

        try:
            completion = client.chat.completions.create(
                model=cfg.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
                temperature=0,
                max_completion_tokens=8000,
                top_p=1,
                stream=False,
                stop=None,
                timeout=cfg.request_timeout_seconds,
            )
            return completion

        except Exception as e:
            last_err = e
            log("WARN", f"Falha na chamada Groq (tentativa {attempt}/{cfg.retries}): {e}")
            if attempt < cfg.retries:
                time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(f"Falha ao chamar Groq após {cfg.retries} tentativas: {last_err}")


# =============================================================================
# 4) PARSE + PERSIST
# =============================================================================

def extract_json_from_text(text: str) -> Dict[str, Any]:
    """
    Extrai o primeiro objeto JSON encontrado na resposta.
    Suporta respostas indevidas com code fences.
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("Resposta da IA vazia.")

    # Remove fenced code blocks (caso o modelo desobedeça)
    if s.startswith("```"):
        s = s.strip("`").strip()
        if s.lower().startswith("json"):
            s = s[4:].strip()
        if s.endswith("```"):
            s = s[:-3].strip()

    first = s.find("{")
    last = s.rfind("}")
    if first == -1 or last == -1 or last <= first:
        raise ValueError("Resposta da IA não contém JSON válido.")

    candidate = s[first:last + 1]
    parsed = json.loads(candidate)
    if not isinstance(parsed, dict):
        raise ValueError("JSON parseado não é um objeto.")
    return parsed


def _repair_json_text(text: str) -> Optional[str]:
    """Tenta reparar erros comuns de JSON (ex.: vírgulas sobrando, cercas de código)."""
    s = (text or "").strip()
    if not s:
        return None

    # Remove cercas de código ```json ... ```
    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s)

    # Isola conteúdo entre a primeira { e a última }
    first = s.find("{")
    last = s.rfind("}")
    if first == -1 or last == -1 or last <= first:
        return None
    candidate = s[first:last + 1].strip()

    # Remove "json" prefixado no começo
    if candidate.lower().startswith("json"):
        candidate = candidate[4:].strip()

    # Remove vírgulas sobrando antes de } ou ]
    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)

    return candidate


def _aggressive_repair_json_text(text: str) -> Optional[str]:
    """Reparo mais agressivo: remove lixo após JSON e tenta converter aspas simples."""
    s = _repair_json_text(text)
    if not s:
        return None

    # Remove qualquer texto após o último "}" (caso tenha ruído)
    last = s.rfind("}")
    if last != -1:
        s = s[: last + 1]

    # Tenta converter strings com aspas simples para aspas duplas (heurística simples)
    s = re.sub(r"'([^'\\]*(?:\\.[^'\\]*)*)'", r'"\1"', s)
    return s


def parse_json_with_repair(text: str) -> Dict[str, Any]:
    """Parseia JSON tentando reparos simples antes de falhar."""
    try:
        return extract_json_from_text(text)
    except Exception:
        repaired = _repair_json_text(text)
        if repaired:
            parsed = json.loads(repaired)
        else:
            aggressive = _aggressive_repair_json_text(text)
            if not aggressive:
                raise
            parsed = json.loads(aggressive)
        if not isinstance(parsed, dict):
            raise ValueError("JSON reparado não é um objeto.")
        return parsed


def parse_and_validate_notes(content: str) -> List[Dict[str, Any]]:
    """Extrai JSON da resposta e valida o schema esperado."""
    parsed = parse_json_with_repair(content)
    case_data = parsed.get("caseData") if isinstance(parsed, dict) else None
    if not isinstance(case_data, dict):
        raise ValueError("JSON não contém objeto 'caseData'.")

    notes_refs = case_data.get("notesReferences")
    if not isinstance(notes_refs, list):
        raise ValueError("JSON não contém 'caseData.notesReferences' como lista.")
    return notes_refs


def _split_text_in_chunks(text: str, max_chars: int = 1500) -> List[str]:
    """Divide o texto em blocos menores respeitando quebras de linha."""
    text = (text or "").strip()
    if not text:
        return []
    parts: List[str] = []
    current: List[str] = []
    current_len = 0
    for block in text.split("\n\n"):
        block = block.strip()
        if not block:
            continue
        block_len = len(block)
        if current_len + block_len + 2 <= max_chars:
            current.append(block)
            current_len += block_len + 2
        else:
            if current:
                parts.append("\n\n".join(current))
            current = [block]
            current_len = block_len
    if current:
        parts.append("\n\n".join(current))
    return parts


def _run_groq_with_retries(
    groq_cfg: GroqCfg,
    chunk_text: str,
) -> List[Dict[str, Any]]:
    """Executa Groq com reparo + retries (normal, estrito, minificado)."""
    completion = call_groq(groq_cfg, chunk_text)
    message = completion.choices[0].message
    content = message.content if hasattr(message, "content") else str(message)
    try:
        return parse_and_validate_notes(content)
    except Exception as e:
        log("WARN", f"Falha ao processar resposta da IA: {e}")

    log("STEP", "Tentando novamente com prompt mais rígido (JSON estrito)")
    completion_retry = call_groq(
        groq_cfg,
        chunk_text,
        system_prompt=STRICT_SYSTEM_PROMPT,
        user_prompt=build_strict_user_prompt(chunk_text),
    )
    message = completion_retry.choices[0].message
    content = message.content if hasattr(message, "content") else str(message)
    try:
        return parse_and_validate_notes(content)
    except Exception as e_retry:
        log("WARN", f"Falha ao processar resposta da IA após retry: {e_retry}")

    log("STEP", "Tentando novamente com prompt minificado (JSON em uma linha)")
    completion_retry2 = call_groq(
        groq_cfg,
        chunk_text,
        system_prompt=STRICT_MINIFIED_SYSTEM_PROMPT,
        user_prompt=build_minified_user_prompt(chunk_text),
    )
    message = completion_retry2.choices[0].message
    content = message.content if hasattr(message, "content") else str(message)
    return parse_and_validate_notes(content)


def count_items(notes_references: List[Dict[str, Any]]) -> Tuple[int, int]:
    """
    Retorna:
    - total_notes: quantidade de blocos noteType/rawLine (len(notesReferences))
    - total_items: soma de items dentro de cada bloco
    """
    total_notes = len(notes_references)
    total_items = 0
    for entry in notes_references:
        items = entry.get("items")
        if isinstance(items, list):
            total_items += len(items)
    return total_notes, total_items


def persist_success(
    col: Collection,
    doc_id: Any,
    *,
    notes_references: List[Dict[str, Any]],
    provider: str,
    model: str,
    latency_ms: int,
) -> None:
    """Persiste sucesso com audit + processing + status."""
    update = {
        # payload final
        "caseData.notesReferences": notes_references,

        # processing
        "processing.caseNotesRefsStatus": "success",
        "processing.caseNotesRefsError": None,
        "processing.caseNotesRefsAt": utc_now(),
        "processing.caseNotesRefsProvider": provider,
        "processing.caseNotesRefsModel": model,
        "processing.caseNotesRefsLatencyMs": latency_ms,
        "processing.pipelineStatus": "notesReferencesExtracted",

        # audit
        "audit.updatedAt": utc_now(),

        # status (pipeline)
        # Observação: mantém um status coerente para a etapa; ajuste conforme sua máquina de estados.
        "status.pipelineStatus": "notesReferencesExtracted",
    }
    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(
    col: Collection,
    doc_id: Any,
    *,
    err: str,
    provider: str,
    model: str,
) -> None:
    """Persiste erro com audit + processing (sem destruir dados existentes)."""
    update = {
        "processing.caseNotesRefsStatus": "error",
        "processing.caseNotesRefsError": err,
        "processing.caseNotesRefsAt": utc_now(),
        "processing.caseNotesRefsProvider": provider,
        "processing.caseNotesRefsModel": model,
        "processing.pipelineStatus": "notesReferencesExtractError",
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": "notesReferencesExtractError",
    }
    col.update_one({"_id": doc_id}, {"$set": update})


# =============================================================================
# 5) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Iniciando extração de referências (notes) via Groq")

    # Mongo
    try:
        col = get_case_data_collection()
    except Exception as e:
        log("ERROR", f"Falha ao conectar no MongoDB: {e}")
        return 1

    # Groq config
    try:
        ai_raw = load_json(AI_MODEL_CONFIG_PATH)
        groq_cfg = build_groq_cfg(ai_raw)
    except Exception as e:
        log("ERROR", f"Falha ao carregar config Groq: {e}")
        return 1

    # Input: identity.stfDecisionId
    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId vazio.")
        return 1

    # Busca documento (projection mínima)
    log("STEP", f"Buscando documento por identity.stfDecisionId='{stf_decision_id}'")
    doc = col.find_one(
        {"identity.stfDecisionId": stf_decision_id},
        projection={
            "identity.stfDecisionId": 1,
            "caseTitle": 1,
            "caseContent.md.notes": 1,
        },
    )

    if not doc:
        log("WARN", f"Nenhum documento encontrado para identity.stfDecisionId='{stf_decision_id}'")
        return 1

    title = (doc.get("caseTitle") or "").strip()
    log("OK", f"Documento encontrado: '{title or '(sem título)'}' | _id={doc.get('_id')}")

    notes_text = (((doc.get("caseContent") or {}).get("md") or {}).get("notes") or "").strip()
    if not notes_text:
        log("WARN", "Campo caseContent.md.notes vazio. Nada a processar.")
        return 1

    chunks = _split_text_in_chunks(notes_text, max_chars=1500)
    if not chunks:
        log("WARN", "Texto de notes vazio após normalização.")
        return 1

    log("STEP", f"Processando em blocos menores | chunks={len(chunks)}")
    all_notes: List[Dict[str, Any]] = []
    started = time.time()
    try:
        for i, chunk in enumerate(chunks, start=1):
            log("STEP", f"Enviando bloco {i}/{len(chunks)} para Groq | chars={len(chunk)}")
            notes_refs = _run_groq_with_retries(groq_cfg, chunk)
            all_notes.extend(notes_refs)

        elapsed_ms = int((time.time() - started) * 1000)
        persist_success(
            col,
            doc.get("_id"),
            notes_references=all_notes,
            provider="groq",
            model=groq_cfg.model,
            latency_ms=elapsed_ms,
        )

        total_notes, total_items = count_items(all_notes)
        log("OK", "Resposta recebida e persistida no MongoDB")
        log("INFO", f"Blocos (notesReferences): {total_notes}")
        log("INFO", f"Itens totais (soma de items): {total_items}")
        return 0
    except Exception as e:
        err = str(e)
        log("ERROR", f"Falha ao processar resposta da IA após blocos: {err}")
        persist_error(col, doc.get("_id"), err=err, provider="groq", model=groq_cfg.model)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
