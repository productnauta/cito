#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step06-extract-legislation-groq.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts legislation references via Groq and stores normalized references.
Inputs: config/mongo.json, config/ai-model.json, caseContent.md.legislation.
Outputs: caseData.legislationReferences + processing/status updates.
Pipeline: chunk text -> Groq extraction (retry/repair) -> persist references.
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
from typing import Any, Dict

from groq import Groq
from pymongo import MongoClient
from pymongo.collection import Collection


# =============================================================================
# 0) LOG
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
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    return json.loads(path.read_text(encoding="utf-8"))


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    m = raw.get("mongo")
    if not isinstance(m, dict):
        raise ValueError("Config invalida: chave 'mongo' ausente ou invalida.")
    uri = str(m.get("uri") or "").strip()
    db = str(m.get("database") or "").strip()
    if not uri:
        raise ValueError("Config invalida: 'mongo.uri' vazio.")
    if not db:
        raise ValueError("Config invalida: 'mongo.database' vazio.")
    return MongoCfg(uri=uri, database=db)


def build_groq_cfg(raw: Dict[str, Any]) -> GroqCfg:
    g = raw.get("groq")
    if not isinstance(g, dict):
        raise ValueError("Config invalida: provider 'groq' ausente em ai-model.json.")
    api_key = str(g.get("api_key") or "").strip()
    model = str(g.get("model") or "").strip()
    if not api_key:
        raise ValueError("Config invalida: 'groq.api_key' vazio.")
    if not model:
        raise ValueError("Config invalida: 'groq.model' vazio.")
    return GroqCfg(
        api_key=api_key,
        model=model,
        request_timeout_seconds=int(g.get("request_timeout_seconds") or 60),
        retries=int(g.get("retries") or 1),
        api_delay_seconds=float(g.get("api_delay_seconds") or 10),
    )


def get_case_data_collection() -> Collection:
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 2) PROMPT
# =============================================================================

SYSTEM_PROMPT = (
    ""
)

# Prompt mais rígido para retry quando houver erro de parsing/JSON inválido.
STRICT_SYSTEM_PROMPT = SYSTEM_PROMPT + (
    "\n### OUTPUT STRICT\n"
    "- Responda APENAS com JSON válido.\n"
    "- Não use Markdown, não use texto fora do JSON.\n"
    "- Verifique vírgulas e aspas.\n"
)

# Prompt ainda mais rígido: JSON estrito e minificado.
STRICT_MINIFIED_SYSTEM_PROMPT = STRICT_SYSTEM_PROMPT + (
    "- Retorne o JSON em uma única linha (minificado).\n"
)


def build_user_prompt(texto_entrada: str) -> str:
    return (
        "Extraia as referencias do texto abaixo no formato JSON definido. "
        "Nao inclua explicacoes ou texto introdutorio.\n\n"
        "TEXTO:\n"
        f"{texto_entrada}"
    )


def build_strict_user_prompt(texto_entrada: str) -> str:
    """Prompt do usuário para retry, reforçando JSON válido."""
    return "RETORNE APENAS JSON VÁLIDO.\n\n" + build_user_prompt(texto_entrada)


def build_minified_user_prompt(texto_entrada: str) -> str:
    """Prompt do usuário para retry com JSON minificado."""
    return "RETORNE APENAS JSON VÁLIDO EM UMA ÚNICA LINHA (MINIFICADO).\n\n" + build_user_prompt(texto_entrada)


# =============================================================================
# 3) GROQ CALL
# =============================================================================

def call_groq(
    cfg: GroqCfg,
    texto_entrada: str,
    *,
    system_prompt: str | None = None,
    user_prompt: str | None = None,
) -> Any:
    client = Groq(api_key=cfg.api_key)

    system_prompt = system_prompt or SYSTEM_PROMPT
    user_prompt = user_prompt or build_user_prompt(texto_entrada)

    last_err: Exception | None = None
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

    raise RuntimeError(f"Falha ao chamar Groq apos {cfg.retries} tentativas: {last_err}")


# =============================================================================
# 4) PARSE + PERSIST
# =============================================================================

def extract_json_from_text(text: str) -> Any:
    s = (text or "").strip()
    if not s:
        raise ValueError("Resposta da IA vazia.")

    # remove fenced code blocks
    if s.startswith("```"):
        s = s.lstrip("`").strip()
        s = s.replace("json", "", 1).strip()
        if s.endswith("```"):
            s = s[:-3].strip()

    first = s.find("{")
    last = s.rfind("}")
    if first == -1 or last == -1 or last <= first:
        raise ValueError("Resposta da IA nao contem JSON valido.")

    candidate = s[first:last + 1]
    return json.loads(candidate)


def _repair_json_text(text: str) -> str | None:
    """Tenta reparar erros comuns de JSON (ex.: vírgulas sobrando, cercas de código)."""
    s = (text or "").strip()
    if not s:
        return None

    if s.startswith("```"):
        s = re.sub(r"^```[a-zA-Z]*\s*", "", s)
        s = re.sub(r"\s*```$", "", s)

    first = s.find("{")
    last = s.rfind("}")
    if first == -1 or last == -1 or last <= first:
        return None

    candidate = s[first:last + 1].strip()
    if candidate.lower().startswith("json"):
        candidate = candidate[4:].strip()

    candidate = re.sub(r",\s*([}\]])", r"\1", candidate)
    return candidate


def _aggressive_repair_json_text(text: str) -> str | None:
    """Reparo mais agressivo: remove lixo após JSON e tenta converter aspas simples."""
    s = _repair_json_text(text)
    if not s:
        return None

    # Remove qualquer texto após o último "}" (caso tenha ruído)
    last = s.rfind("}")
    if last != -1:
        s = s[: last + 1]

    # Tenta converter strings com aspas simples para aspas duplas (heurística simples)
    # Atenção: isso é heurístico e pode falhar, mas ajuda em respostas com aspas simples.
    s = re.sub(r"'([^'\\]*(?:\\.[^'\\]*)*)'", r'"\1"', s)
    return s


def parse_json_with_repair(text: str) -> Dict[str, Any]:
    """Parseia JSON tentando reparos simples antes de falhar."""
    try:
        parsed = extract_json_from_text(text)
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
        raise ValueError("JSON parseado não é um objeto.")
    return parsed


def parse_and_validate_legislation(content: str) -> list:
    """Extrai JSON da resposta e valida o schema esperado."""
    parsed = parse_json_with_repair(content)
    refs = ((parsed.get("caseData") or {}).get("legislationReferences")) if isinstance(parsed, dict) else None
    if not isinstance(refs, list):
        raise ValueError("JSON nao contem caseData.legislationReferences como lista.")
    return refs


def _split_text_in_chunks(text: str, max_chars: int = 1500) -> list[str]:
    """Divide o texto em blocos menores respeitando quebras de linha."""
    text = (text or "").strip()
    if not text:
        return []
    parts = []
    current = []
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
) -> list:
    """Executa Groq com reparo + retries (normal, estrito, minificado)."""
    # 1) tentativa normal
    started = time.time()
    completion = call_groq(groq_cfg, chunk_text)
    message = completion.choices[0].message
    content = message.content if hasattr(message, "content") else str(message)
    try:
        return parse_and_validate_legislation(content)
    except Exception as e:
        log("WARN", f"Falha ao processar resposta da IA: {e}")

    # 2) retry estrito
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
        return parse_and_validate_legislation(content)
    except Exception as e_retry:
        log("WARN", f"Falha ao processar resposta da IA após retry: {e_retry}")

    # 3) retry minificado
    log("STEP", "Tentando novamente com prompt minificado (JSON em uma linha)")
    completion_retry2 = call_groq(
        groq_cfg,
        chunk_text,
        system_prompt=STRICT_MINIFIED_SYSTEM_PROMPT,
        user_prompt=build_minified_user_prompt(chunk_text),
    )
    message = completion_retry2.choices[0].message
    content = message.content if hasattr(message, "content") else str(message)
    return parse_and_validate_legislation(content)


def persist_success(
    col: Collection,
    doc_id: Any,
    *,
    refs: list,
    provider: str,
    model: str,
    latency_ms: int,
) -> None:
    update = {
        "caseData.legislationReferences": refs,
        "processing.caseLegislationRefsStatus": "success",
        "processing.caseLegislationRefsError": None,
        "processing.caseLegislationRefsAt": utc_now(),
        "processing.caseLegislationRefsProvider": provider,
        "processing.caseLegislationRefsModel": model,
        "processing.caseLegislationRefsLatencyMs": latency_ms,
        "processing.pipelineStatus": "legislationExtracted",
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": "legislationExtracted",
    }
    col.update_one({"_id": doc_id}, {"$set": update})


def persist_error(col: Collection, doc_id: Any, *, err: str, provider: str, model: str) -> None:
    update = {
        "processing.caseLegislationRefsStatus": "error",
        "processing.caseLegislationRefsError": err,
        "processing.caseLegislationRefsAt": utc_now(),
        "processing.caseLegislationRefsProvider": provider,
        "processing.caseLegislationRefsModel": model,
        "processing.pipelineStatus": "legislationExtractError",
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": "legislationExtractError",
    }
    col.update_one({"_id": doc_id}, {"$set": update})


# =============================================================================
# 5) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Iniciando extracao legislativa via Groq")

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

    stf_decision_id = input("Informe o identity.stfDecisionId: ").strip()
    if not stf_decision_id:
        log("ERROR", "identity.stfDecisionId vazio.")
        return 1

    log("STEP", f"Buscando documento por identity.stfDecisionId='{stf_decision_id}'")
    doc = col.find_one(
        {"identity.stfDecisionId": stf_decision_id},
        projection={"caseContent.md.legislation": 1, "identity.stfDecisionId": 1, "caseTitle": 1},
    )

    if not doc:
        log("WARN", f"Nenhum documento encontrado para identity.stfDecisionId='{stf_decision_id}'")
        return 1

    legislation_text = (((doc.get("caseContent") or {}).get("md") or {}).get("legislation") or "").strip()
    if not legislation_text:
        log("WARN", "Campo caseContent.md.legislation vazio.")
        return 1

    chunks = _split_text_in_chunks(legislation_text, max_chars=1500)
    if not chunks:
        log("WARN", "Texto de legislação vazio após normalização.")
        return 1

    log("STEP", f"Processando em blocos menores | chunks={len(chunks)}")
    all_refs: list = []
    started = time.time()
    try:
        for i, chunk in enumerate(chunks, start=1):
            log("STEP", f"Enviando bloco {i}/{len(chunks)} para Groq | chars={len(chunk)}")
            refs = _run_groq_with_retries(groq_cfg, chunk)
            all_refs.extend(refs)

        elapsed_ms = int((time.time() - started) * 1000)
        persist_success(
            col,
            doc.get("_id"),
            refs=all_refs,
            provider="groq",
            model=groq_cfg.model,
            latency_ms=elapsed_ms,
        )

        log("OK", "Resposta recebida e persistida no MongoDB")
        log("INFO", f"Itens identificados e extraidos: {len(all_refs)}")
        return 0
    except Exception as e:
        err = str(e)
        log("ERROR", f"Falha ao processar resposta da IA após blocos: {err}")
        persist_error(col, doc.get("_id"), err=err, provider="groq", model=groq_cfg.model)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
