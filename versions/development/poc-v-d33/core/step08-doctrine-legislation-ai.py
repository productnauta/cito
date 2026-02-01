#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step08-doctrine-legislation-ai.py
Version: poc-v-d33      Date: 2024-05-20 (data de criação/versionamento)
Author:  Chico Alff     Rep: https://github.com/pigmeu-labs/cito
-----------------------------------------------------------------------------------------------------
Description: Extracts doctrinal citations via Groq and stores normalized doctrine references.
Inputs: config/mongo.json, config/ai-model.json, caseContent.md.doctrine.
Outputs: caseData.caseDoctrines + processing/status updates.
Pipeline: chunk text -> Groq extraction (retry/repair) -> schema validation -> persist doctrines.
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
# 1) PATHS / CONSTANTS
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.json"
AI_MODEL_CONFIG_PATH = CONFIG_DIR / "ai-model.json"

CASE_DATA_COLLECTION = "case_data"
EXPECTED_PROVIDER = "groq"
EXPECTED_MODEL = "llama-3.1-8b-instant"


# =============================================================================
# 2) CONFIG MODELS
# =============================================================================

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
    """Interpreta config/ai-model.json (provider groq)."""
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
    """Conecta ao MongoDB e retorna a collection case_data."""
    log("STEP", f"Lendo config MongoDB: {MONGO_CONFIG_PATH.resolve()}")
    raw = load_json(MONGO_CONFIG_PATH)
    cfg = build_mongo_cfg(raw)

    log("STEP", "Conectando ao MongoDB")
    client = MongoClient(cfg.uri)

    log("OK", f"MongoDB OK | db='{cfg.database}' | collection='{CASE_DATA_COLLECTION}'")
    return client[cfg.database][CASE_DATA_COLLECTION]


# =============================================================================
# 3) PROMPTS (DOCTRINE EXTRACTION)
# =============================================================================

SYSTEM_PROMPT = (
    "Você é um agente especializado em extração estruturada de citações doutrinárias\n"
    "a partir de textos jurídicos em português brasileiro.\n\n"
    "Objetivo: identificar, separar e normalizar CADA citação doutrinária do texto.\n\n"
    "REGRAS:\n\n"
    "1) Separação de citações\n"
    "- Cada item pode estar separado por quebra de linha, ponto final, ou conter múltiplas citações na mesma linha.\n"
    "- Se houver duas citações coladas (ex.: termina em \"p. X.\" e já inicia outro AUTOR), separe em itens distintos.\n\n"
    "2) Campos a extrair (por citação)\n"
    "- author: autor(es) conforme aparece(m)\n"
    "- publicationTitle: título do documento citado (livro, artigo, capítulo, verbete, peça etc.)\n"
    "- edition: edição (ex.: \"4 ed\"), se existir\n"
    "- publicationPlace: local\n"
    "- publisher: editora\n"
    "- year: ano (int, 4 dígitos) ou null se ausente/ambíguo\n"
    "- page: páginas citadas (ex.: \"181\", \"233-234\", \"233-234 e 1.561\") ou null\n"
    "- rawCitation: citação original completa (exatamente o trecho do input)\n\n"
    "3) Regras específicas — capítulo / obra coletiva\n"
    "Quando a citação tiver padrão do tipo:\n"
    "- \"Título do capítulo. p. X-Y. In: Título do livro/obra coletiva. (Org./Coord./Ed.). Local: Editora, Ano.\"\n"
    "Faça:\n"
    "- author = autor(es) antes do primeiro ponto (ou conforme padrão autor)\n"
    "- publicationTitle = título do capítulo (o que vem antes do \"In:\")\n"
    "- edition = null (a menos que exista explicitamente para a obra onde a edição é indicada)\n"
    "- publicationPlace / publisher / year = preferencialmente os dados da obra coletiva (após \"In:\")\n"
    "- page = páginas do capítulo (normalmente após \"p.\")\n"
    "- NÃO crie novos campos (ex.: não adicionar \"containerTitle\", \"organizer\", \"coordinator\").\n"
    "  Se houver organizador/coordenador, apenas ignore ou mantenha dentro de rawCitation.\n\n"
    "4) Normalização\n"
    "- Não invente dados ausentes → use null\n"
    "- Preserve acentos e grafia original\n"
    "- Não traduza títulos\n"
    "- Não “conserte” nomes\n"
    "- Se houver múltiplos autores, preserve como aparece (ex.: \"AUTOR1; AUTOR2\")\n\n"
    "5) Escopo\n"
    "- Extrair somente DOUTRINA (livros, capítulos, artigos, obras)\n"
    "- Ignorar legislação, jurisprudência, notas editoriais não bibliográficas\n\n"
    "6) Saída\n"
    "- Responda APENAS JSON válido\n"
    "- Sem markdown, sem comentários, sem texto extra\n"
    "- Use EXATAMENTE o schema abaixo:\n\n"
    "{\n"
    "  \"caseData\": {\n"
    "    \"caseDoctrines\": [\n"
    "      {\n"
    "        \"author\": \"string\",\n"
    "        \"publicationTitle\": \"string\",\n"
    "        \"edition\": \"string|null\",\n"
    "        \"publicationPlace\": \"string|null\",\n"
    "        \"publisher\": \"string|null\",\n"
    "        \"year\": 0,\n"
    "        \"page\": \"string|null\",\n"
    "        \"rawCitation\": \"string\"\n"
    "      }\n"
    "    ]\n"
    "  }\n"
    "}\n"
)

# Prompt mais rígido para retry quando houver erro de parsing/JSON inválido.
STRICT_SYSTEM_PROMPT = SYSTEM_PROMPT + (
    "\n# OUTPUT STRICT\n"
    "- Responda APENAS com JSON válido.\n"
    "- Não use Markdown, não use texto fora do JSON.\n"
    "- Verifique vírgulas e aspas.\n"
)

# Prompt ainda mais rígido: JSON estrito e minificado.
STRICT_MINIFIED_SYSTEM_PROMPT = STRICT_SYSTEM_PROMPT + (
    "- Retorne o JSON em uma única linha (minificado).\n"
)


def build_user_prompt(doctrine_text: str) -> str:
    """Monta o prompt do usuário inserindo o texto de Doutrina."""
    return (
        "Extraia todas as citações doutrinárias do texto abaixo e retorne APENAS o JSON no schema solicitado.\n\n"
        "TEXTO:\n"
        f"{doctrine_text}\n"
    )


def build_strict_user_prompt(doctrine_text: str) -> str:
    """Prompt do usuário para retry, reforçando JSON válido."""
    return "RETORNE APENAS JSON VÁLIDO.\n\n" + build_user_prompt(doctrine_text)


def build_minified_user_prompt(doctrine_text: str) -> str:
    """Prompt do usuário para retry com JSON minificado."""
    return "RETORNE APENAS JSON VÁLIDO EM UMA ÚNICA LINHA (MINIFICADO).\n\n" + build_user_prompt(doctrine_text)


# =============================================================================
# 4) GROQ CALL
# =============================================================================

def call_groq(
    cfg: GroqCfg,
    doctrine_text: str,
    *,
    system_prompt: Optional[str] = None,
    user_prompt: Optional[str] = None,
) -> Any:
    """Chamada à Groq com retry/backoff simples e timeout."""
    client = Groq(api_key=cfg.api_key)

    system_prompt = system_prompt or SYSTEM_PROMPT
    user_prompt = user_prompt or build_user_prompt(doctrine_text)

    last_err: Optional[Exception] = None
    for attempt in range(1, cfg.retries + 1):
        if cfg.api_delay_seconds and attempt > 1:
            log("STEP", f"Aguardando api_delay_seconds={cfg.api_delay_seconds}s antes do retry")
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
# 5) PARSE + VALIDATION
# =============================================================================

def extract_json_from_text(text: str) -> Dict[str, Any]:
    """
    Extrai o primeiro objeto JSON encontrado na resposta.
    Tolera respostas indevidas com code fences.
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


def parse_and_validate_doctrines(content: str) -> List[Dict[str, Any]]:
    """Extrai JSON da resposta e valida o schema esperado."""
    parsed = parse_json_with_repair(content)
    return validate_schema(parsed)


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
        return parse_and_validate_doctrines(content)
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
        return parse_and_validate_doctrines(content)
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
    return parse_and_validate_doctrines(content)


def _is_str_or_none(v: Any) -> bool:
    return v is None or isinstance(v, str)


def _is_int_year_or_none(v: Any) -> bool:
    if v is None:
        return True
    if not isinstance(v, int):
        return False
    return 1000 <= v <= 2100


def validate_schema(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Valida o schema:
    {
      "caseData": {
        "caseDoctrines": [ { ... } ]
      }
    }
    Retorna a lista caseDoctrines já validada.
    """
    if not isinstance(payload, dict):
        raise ValueError("Payload não é objeto JSON.")

    case_data = payload.get("caseData")
    if not isinstance(case_data, dict):
        raise ValueError("JSON não contém objeto 'caseData'.")

    doctrines = case_data.get("caseDoctrines")
    if not isinstance(doctrines, list):
        raise ValueError("JSON não contém 'caseData.caseDoctrines' como lista.")

    required_keys = {
        "author",
        "publicationTitle",
        "edition",
        "publicationPlace",
        "publisher",
        "year",
        "page",
        "rawCitation",
    }

    for i, item in enumerate(doctrines):
        if not isinstance(item, dict):
            raise ValueError(f"caseDoctrines[{i}] não é objeto.")
        missing = [k for k in required_keys if k not in item]
        if missing:
            raise ValueError(f"caseDoctrines[{i}] faltando campos: {missing}")

        if not isinstance(item.get("author"), str) or not item["author"].strip():
            raise ValueError(f"caseDoctrines[{i}].author inválido.")
        if not isinstance(item.get("publicationTitle"), str) or not item["publicationTitle"].strip():
            raise ValueError(f"caseDoctrines[{i}].publicationTitle inválido.")
        if not _is_str_or_none(item.get("edition")):
            raise ValueError(f"caseDoctrines[{i}].edition deve ser string ou null.")
        if not _is_str_or_none(item.get("publicationPlace")):
            raise ValueError(f"caseDoctrines[{i}].publicationPlace deve ser string ou null.")
        if not _is_str_or_none(item.get("publisher")):
            raise ValueError(f"caseDoctrines[{i}].publisher deve ser string ou null.")
        if not _is_int_year_or_none(item.get("year")):
            raise ValueError(f"caseDoctrines[{i}].year deve ser int(4 dígitos) ou null.")
        if not _is_str_or_none(item.get("page")):
            raise ValueError(f"caseDoctrines[{i}].page deve ser string ou null.")
        if not isinstance(item.get("rawCitation"), str) or not item["rawCitation"].strip():
            raise ValueError(f"caseDoctrines[{i}].rawCitation inválido.")

    return doctrines


def count_doctrines(doctrines: List[Dict[str, Any]]) -> int:
    return len(doctrines)


# =============================================================================
# 6) PERSISTENCE
# =============================================================================

def persist_success(
    col: Collection,
    doc_id: Any,
    *,
    doctrines: List[Dict[str, Any]],
    provider: str,
    model: str,
    latency_ms: int,
) -> None:
    """Persiste o array em caseData.caseDoctrines + metadados de processing/audit/status."""
    update = {
        # payload final
        "caseData.caseDoctrines": doctrines,

        # processing
        "processing.caseDoctrineStatus": "success",
        "processing.caseDoctrineError": None,
        "processing.caseDoctrineAt": utc_now(),
        "processing.caseDoctrineProvider": provider,
        "processing.caseDoctrineModel": model,
        "processing.caseDoctrineLatencyMs": latency_ms,
        "processing.caseDoctrineCount": len(doctrines),
        "processing.pipelineStatus": "doctrineExtracted",

        # audit
        "audit.updatedAt": utc_now(),

        # status (pipeline)
        "status.pipelineStatus": "doctrineExtracted",
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
    """Persiste erro em processing/audit (sem destruir dados existentes)."""
    update = {
        "processing.caseDoctrineStatus": "error",
        "processing.caseDoctrineError": err,
        "processing.caseDoctrineAt": utc_now(),
        "processing.caseDoctrineProvider": provider,
        "processing.caseDoctrineModel": model,
        "processing.pipelineStatus": "doctrineExtractError",
        "audit.updatedAt": utc_now(),
        "status.pipelineStatus": "doctrineExtractError",
    }
    col.update_one({"_id": doc_id}, {"$set": update})


# =============================================================================
# 7) MAIN
# =============================================================================

def main() -> int:
    log("INFO", "Iniciando extração de citações doutrinárias (doctrine) via Groq")

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

    # Força/valida provider e model conforme requisito
    if groq_cfg.model != EXPECTED_MODEL:
        log("WARN", f"Modelo em config difere do esperado: config='{groq_cfg.model}' esperado='{EXPECTED_MODEL}'")
        # Mantém o requisito: usar llama-3.1-8b-instant
        groq_cfg = GroqCfg(
            api_key=groq_cfg.api_key,
            model=EXPECTED_MODEL,
            request_timeout_seconds=groq_cfg.request_timeout_seconds,
            retries=groq_cfg.retries,
            api_delay_seconds=groq_cfg.api_delay_seconds,
        )

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
            "caseContent.md.doctrine": 1,
        },
    )

    if not doc:
        log("WARN", f"Nenhum documento encontrado para identity.stfDecisionId='{stf_decision_id}'")
        return 1

    doc_id = doc.get("_id")
    title = (doc.get("caseTitle") or "").strip()
    log("OK", f"Documento encontrado: '{title or '(sem título)'}' | _id={doc_id}")

    doctrine_text = (((doc.get("caseContent") or {}).get("md") or {}).get("doctrine") or "").strip()
    if not doctrine_text:
        log("WARN", "Campo caseContent.md.doctrine vazio. Nada a processar.")
        return 1

    chunks = _split_text_in_chunks(doctrine_text, max_chars=1500)
    if not chunks:
        log("WARN", "Texto de doctrine vazio após normalização.")
        return 1

    log("STEP", f"Processando em blocos menores | chunks={len(chunks)}")
    all_doctrines: List[Dict[str, Any]] = []
    started = time.time()
    try:
        for i, chunk in enumerate(chunks, start=1):
            log(
                "STEP",
                f"Enviando bloco {i}/{len(chunks)} para Groq | chars={len(chunk)} | model='{groq_cfg.model}'",
            )
            doctrines = _run_groq_with_retries(groq_cfg, chunk)
            all_doctrines.extend(doctrines)

        elapsed_ms = int((time.time() - started) * 1000)
        log("STEP", "Persistindo em caseData.caseDoctrines")
        persist_success(
            col,
            doc_id,
            doctrines=all_doctrines,
            provider=EXPECTED_PROVIDER,
            model=groq_cfg.model,
            latency_ms=elapsed_ms,
        )

        log("OK", "Persistência concluída")
        log("INFO", f"Total de citações (caseDoctrines): {count_doctrines(all_doctrines)}")
        log("INFO", "Status pipeline atualizado: status.pipelineStatus='doctrineExtracted'")
        return 0

    except Exception as e:
        err = str(e)
        log("ERROR", f"Falha ao processar resposta da IA após blocos: {err}")
        if doc_id is not None:
            persist_error(col, doc_id, err=err, provider=EXPECTED_PROVIDER, model=groq_cfg.model)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
