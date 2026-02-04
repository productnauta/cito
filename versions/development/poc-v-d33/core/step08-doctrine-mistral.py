#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step08-doctrine-legislation-ai.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Extracts doctrinal citations via Mistral and stores structured doctrine references.
Inputs: config/mongo.yaml, config/providers.yaml, config/prompts.yaml, caseContent.md.doctrine.
Outputs: caseData.doctrineReferences + processing/status updates.
Pipeline: load config -> query Mongo -> Mistral extraction -> parse JSON -> persist.
Dependencies: pymongo, requests, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from pymongo.collection import Collection

from utils.mongo import get_case_data_collection
from utils.work_normalize import canonicalize_work, load_alias_map

# =============================================================================
# 0) LOG
# =============================================================================

def _ts() -> str:
    # Requisito: yy-mm-dd hh:mm:ss
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def log(msg: str) -> None:
    print(f"[{_ts()}] - {msg}")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


# =============================================================================
# 1) CONFIG
# =============================================================================

BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"

MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
PROVIDERS_CONFIG_PATH = CONFIG_DIR / "providers.yaml"
PROMPTS_CONFIG_PATH = CONFIG_DIR / "prompts.yaml"

CASE_DATA_COLLECTION = "case_data"

# Definir provider e prompt alterando apenas estas variaveis
PROVIDER_NAME = "mistral"
PROVIDER_KEY_NAME = "cito-dev-a"
PROMPT_ID = "extract-doctrines-from-md"

WORK_ALIAS_PATH = CONFIG_DIR / "work_aliases.yaml"
WORK_ALIAS_MAP = load_alias_map(WORK_ALIAS_PATH)


@dataclass(frozen=True)
class MongoCfg:
    uri: str
    database: str


@dataclass(frozen=True)
class ProviderCfg:
    name: str
    api_key: str
    model: str
    temperature: float
    max_tokens: int
    top_p: float
    request_timeout_seconds: int
    retries: int
    api_delay_seconds: float


@dataclass(frozen=True)
class PromptCfg:
    template: List[Dict[str, str]]
    template_variables: List[str]
    client_parameters: Dict[str, Any]


def load_yaml(path: Path) -> Dict[str, Any]:
    # Carrega um YAML simples com validacao minima
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _list_to_dict(items: Any) -> Dict[str, Any]:
    # Converte lista de dicts (ex.: [{temperature: 0}, {max_tokens: 800}]) em dict
    if not isinstance(items, list):
        return {}
    out: Dict[str, Any] = {}
    for item in items:
        if isinstance(item, dict):
            for k, v in item.items():
                out[str(k)] = v
    return out


def build_mongo_cfg(raw: Dict[str, Any]) -> MongoCfg:
    # Extrai dados de conexao do mongo.yaml
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


def build_provider_cfg(raw: Dict[str, Any], provider_name: str, key_name: str) -> ProviderCfg:
    # Carrega configuracao do provider e resolve a chave selecionada
    providers = raw.get("providers")
    if not isinstance(providers, list):
        raise ValueError("Config invalida: 'providers' ausente ou invalido.")

    provider = next((p for p in providers if str(p.get("name")).strip() == provider_name), None)
    if not isinstance(provider, dict):
        raise ValueError(f"Provider '{provider_name}' nao encontrado em providers.yaml.")

    defaults = provider.get("defaults") if isinstance(provider.get("defaults"), dict) else {}
    keys = provider.get("keys") if isinstance(provider.get("keys"), list) else []

    api_key = None
    if key_name:
        for k in keys:
            if str(k.get("name")).strip() == key_name:
                api_key = str(k.get("key") or "").strip()
                break
    if not api_key and keys:
        api_key = str(keys[0].get("key") or "").strip()

    if not api_key:
        raise ValueError(f"Chave de API nao encontrada para provider '{provider_name}'.")

    model = str(defaults.get("model") or "").strip()
    if not model:
        raise ValueError(f"Provider '{provider_name}' sem model em defaults.")

    return ProviderCfg(
        name=provider_name,
        api_key=api_key,
        model=model,
        temperature=float(defaults.get("temperature") or 0),
        max_tokens=int(defaults.get("max_tokens") or 1024),
        top_p=float(defaults.get("top_p") or 1),
        request_timeout_seconds=int(defaults.get("request_timeout_seconds") or 60),
        retries=int(defaults.get("retries") or 1),
        api_delay_seconds=float(defaults.get("api_delay_seconds") or 0),
    )


def build_prompt_cfg(raw: Dict[str, Any], prompt_id: str) -> PromptCfg:
    # Carrega o prompt por id e monta a configuracao do cliente
    prompts = raw.get("prompts")
    if not isinstance(prompts, dict):
        raise ValueError("Config invalida: 'prompts' ausente ou invalido.")

    prompt = prompts.get(prompt_id)
    if not isinstance(prompt, dict):
        raise ValueError(f"Prompt '{prompt_id}' nao encontrado em prompts.yaml.")

    template = prompt.get("template") if isinstance(prompt.get("template"), list) else []
    template_variables = prompt.get("template_variables") if isinstance(prompt.get("template_variables"), list) else []
    client_parameters = _list_to_dict(prompt.get("client_parameters"))

    if not template:
        raise ValueError(f"Prompt '{prompt_id}' sem template.")

    return PromptCfg(
        template=template,
        template_variables=[str(v) for v in template_variables],
        client_parameters=client_parameters,
    )


def get_case_data_collection_local() -> Collection:
    # Usa o utilitario padrao para abrir a collection case_data
    return get_case_data_collection(MONGO_CONFIG_PATH, CASE_DATA_COLLECTION)


# =============================================================================
# 2) PROMPT BUILD
# =============================================================================

def render_prompt(template: List[Dict[str, str]], variables: Dict[str, str]) -> List[Dict[str, str]]:
    # Substitui {{variavel}} no content do template
    rendered: List[Dict[str, str]] = []
    for msg in template:
        role = str(msg.get("role") or "").strip()
        content = str(msg.get("content") or "")
        for key, value in variables.items():
            content = content.replace("{{" + key + "}}", value)
        rendered.append({"role": role, "content": content})
    return rendered


# =============================================================================
# 3) MISTRAL CALL
# =============================================================================

def call_mistral(cfg: ProviderCfg, messages: List[Dict[str, str]], params: Dict[str, Any]) -> Dict[str, Any]:
    # Aplica parametros de prompt sobre defaults do provider
    temperature = float(params.get("temperature", cfg.temperature))
    max_tokens = int(params.get("max_tokens", cfg.max_tokens))
    top_p = float(params.get("top_p", cfg.top_p))

    payload = {
        "model": cfg.model,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        "top_p": top_p,
    }

    headers = {
        "Authorization": f"Bearer {cfg.api_key}",
        "Content-Type": "application/json",
    }

    last_err: Optional[Exception] = None
    for attempt in range(1, cfg.retries + 1):
        if cfg.api_delay_seconds and attempt > 1:
            time.sleep(cfg.api_delay_seconds)
        try:
            resp = requests.post(
                "https://api.mistral.ai/v1/chat/completions",
                json=payload,
                headers=headers,
                timeout=cfg.request_timeout_seconds,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text}")
            return resp.json()
        except Exception as e:
            last_err = e
            log(f"Falha na chamada Mistral (tentativa {attempt}/{cfg.retries}): {e}")
            if attempt < cfg.retries:
                time.sleep(min(2 ** (attempt - 1), 8))

    raise RuntimeError(f"Falha ao chamar Mistral apos {cfg.retries} tentativas: {last_err}")


# =============================================================================
# 4) PARSE LINE PROTOCOL
# =============================================================================

def _to_int_or_none(value: str) -> Optional[int]:
    s = (value or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _to_str_or_none(value: str) -> Optional[str]:
    s = (value or "").strip()
    return s if s else None


def parse_doctrine_protocol(text: str) -> List[Dict[str, Any]]:
    """
    Converte protocolo CITO-DOCTRINE/1 em lista de doctrineReferences.
    Formato por linha: C|author|publicationTitle|edition|publicationPlace|publisher|year|page|rawCitation
    """
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    output: List[Dict[str, Any]] = []

    for line in lines:
        if not line.startswith("C|"):
            continue
        parts = line.split("|", 8)
        # Garante 9 campos (tag + 8)
        while len(parts) < 9:
            parts.append("")

        author = _to_str_or_none(parts[1])
        title = _to_str_or_none(parts[2])
        edition = _to_str_or_none(parts[3])
        place = _to_str_or_none(parts[4])
        publisher = _to_str_or_none(parts[5])
        year = _to_int_or_none(parts[6])
        page = _to_str_or_none(parts[7])
        raw = (parts[8] or "").strip()

        if not raw and not (author or title):
            continue

        output.append(
            {
                "author": author or "",
                "publicationTitle": title or "",
                "edition": edition,
                "publicationPlace": place,
                "publisher": publisher,
                "year": year,
                "page": page,
                "rawCitation": raw or "",
            }
        )

    return output


def _apply_work_keys(refs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    output: List[Dict[str, Any]] = []
    for ref in refs:
        if not isinstance(ref, dict):
            continue
        raw_title = str(ref.get("publicationTitle") or "").strip()
        canonical = canonicalize_work(raw_title, WORK_ALIAS_MAP)
        ref["publicationTitleRaw"] = raw_title
        ref["publicationTitleNorm"] = canonical.get("normTitle") or ""
        ref["workKey"] = canonical.get("workKey") or ""
        ref["workMatchType"] = canonical.get("matchType") or "normalized"
        display = canonical.get("displayTitle") or raw_title
        ref["publicationTitleDisplay"] = display
        output.append(ref)
    return output


# =============================================================================
# 5) PERSISTENCE
# =============================================================================

def _update_processing_success(
    collection: Collection,
    doc_id: Any,
    model: str,
    latency_ms: int,
    refs: List[Dict[str, Any]],
) -> None:
    # Persistencia seguindo o manual (Step08 - Doutrina)
    now = utc_now()
    update = {
        "$set": {
            "caseData.doctrineReferences": refs,
            "processing.caseDoctrineStatus": "success",
            "processing.caseDoctrineError": None,
            "processing.caseDoctrineAt": now,
            "processing.caseDoctrineProvider": PROVIDER_NAME,
            "processing.caseDoctrineModel": model,
            "processing.caseDoctrineLatencyMs": latency_ms,
            "processing.caseDoctrineCount": len(refs),
            "processing.pipelineStatus": "doctrineExtracted",
            "status.pipelineStatus": "doctrineExtracted",
            "audit.updatedAt": now,
        }
    }
    collection.update_one({"_id": doc_id}, update)


def _update_processing_error(collection: Collection, doc_id: Any, model: str, error_msg: str) -> None:
    # Persistencia de erro seguindo o manual
    now = utc_now()
    update = {
        "$set": {
            "processing.caseDoctrineStatus": "error",
            "processing.caseDoctrineError": error_msg,
            "processing.caseDoctrineAt": now,
            "processing.caseDoctrineProvider": PROVIDER_NAME,
            "processing.caseDoctrineModel": model,
            "processing.pipelineStatus": "doctrineExtractError",
            "status.pipelineStatus": "doctrineExtractError",
            "audit.updatedAt": now,
        }
    }
    collection.update_one({"_id": doc_id}, update)


# =============================================================================
# 6) MAIN
# =============================================================================

def main() -> int:
    log("INICIANDO EXECUCAO DO SCRIPT")

    # Input via CLI ou prompt interativo
    parser = argparse.ArgumentParser(description="Extrai citacoes doutrinarias via Mistral")
    parser.add_argument("--stfDecisionId", dest="stf_decision_id", help="ID da decisao STF")
    args = parser.parse_args()

    stf_decision_id = args.stf_decision_id
    if not stf_decision_id:
        stf_decision_id = input("Informe o stfDecisionId: ").strip()

    if not stf_decision_id:
        log("stfDecisionId nao informado. Encerrando.")
        return 1

    # Carregar configuracoes
    log("Carregando configuracoes YAML")
    try:
        mongo_cfg = build_mongo_cfg(load_yaml(MONGO_CONFIG_PATH))
        provider_cfg = build_provider_cfg(load_yaml(PROVIDERS_CONFIG_PATH), PROVIDER_NAME, PROVIDER_KEY_NAME)
        prompt_cfg = build_prompt_cfg(load_yaml(PROMPTS_CONFIG_PATH), PROMPT_ID)
        log(
            "IA em uso | "
            f"PROVIDER={PROVIDER_NAME} | PROVIDER_KEY_NAME={PROVIDER_KEY_NAME} | "
            f"MODEL={provider_cfg.model} | Temperature={provider_cfg.temperature}"
        )
        log(f"MongoDB config OK | database='{mongo_cfg.database}'")
        log(f"Provider OK | name='{provider_cfg.name}' | model='{provider_cfg.model}'")
        log(f"Prompt OK | id='{PROMPT_ID}'")
    except Exception as e:
        log(f"Erro ao carregar configuracoes: {e}")
        return 1

    # Conectar ao MongoDB
    collection = get_case_data_collection_local()

    # Buscar documento
    log(f"Buscando documento para stfDecisionId='{stf_decision_id}'")
    query = {"$or": [{"stfDecisionId": stf_decision_id}, {"identity.stfDecisionId": stf_decision_id}]}
    doc = collection.find_one(query)

    if not doc:
        log("Decisao nao encontrada.")
        return 0

    doc_id = doc.get("_id")
    case_content = doc.get("caseContent") or {}
    md = case_content.get("md") if isinstance(case_content, dict) else {}
    doctrine_text = ""
    if isinstance(md, dict):
        doctrine_text = md.get("doctrine") or ""

    if doctrine_text:
        log("Campo caseContent.md.doctrine encontrado. Conteudo:")
        print(doctrine_text)
    else:
        log("Campo caseContent.md.doctrine nao encontrado. Encerrando.")
        return 0

    # Montar prompt
    log(f"Preparando prompt '{PROMPT_ID}'")
    variables = {var: doctrine_text for var in prompt_cfg.template_variables}
    messages = render_prompt(prompt_cfg.template, variables)

    # Chamar Mistral
    log("Chamando API Mistral")
    start = time.monotonic()
    try:
        response = call_mistral(provider_cfg, messages, prompt_cfg.client_parameters)
        latency_ms = int((time.monotonic() - start) * 1000)
    except Exception as e:
        log(f"Erro na chamada Mistral: {e}")
        if doc_id is not None:
            _update_processing_error(collection, doc_id, provider_cfg.model, str(e))
        return 1

    # Extrair conteudo
    content = ""
    try:
        content = response.get("choices", [{}])[0].get("message", {}).get("content", "")
    except Exception:
        content = ""

    log("Resposta da Mistral recebida:")
    print(content)

    # Parsear protocolo de linhas
    log("Processando resposta para gerar JSON estruturado")
    try:
        refs = parse_doctrine_protocol(content)
        refs = _apply_work_keys(refs)
    except Exception as e:
        log(f"Erro ao parsear resposta: {e}")
        if doc_id is not None:
            _update_processing_error(collection, doc_id, provider_cfg.model, f"parse_error: {e}")
        return 1

    # Persistir no MongoDB
    log("Salvando caseData.doctrineReferences no MongoDB")
    _update_processing_success(collection, doc_id, provider_cfg.model, latency_ms, refs)

    log("FINALIZADO COM SUCESSO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
