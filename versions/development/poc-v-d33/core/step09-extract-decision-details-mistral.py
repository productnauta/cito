#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step09-extract-decision-details-mistral.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Extracts decision details from STF decisions via Mistral and stores decisionDetails.
Inputs: config/mongo.yaml, config/providers.yaml, config/prompts.yaml, caseContent.md.decision.
Outputs: caseData.decisionDetails + processing/status updates.
Pipeline: load config -> query Mongo -> Mistral extraction -> parse JSON -> persist.
Dependencies: pymongo, requests, pyyaml
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml
from pymongo.collection import Collection

from utils.mongo import get_case_data_collection

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
PROVIDER_KEY_NAME = "cito-dev-c"
PROMPT_ID = "get_decision-details-stf"


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
# 4) PARSE JSON
# =============================================================================

def _repair_json_text(text: str) -> Optional[str]:
    # Tenta reparar erros comuns de JSON (cercas de codigo, virgulas extras)
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


def parse_json_with_repair(text: str) -> Dict[str, Any]:
    # Parseia JSON tentando reparos simples antes de falhar
    s = (text or "").strip()
    if not s:
        raise ValueError("Resposta da IA vazia.")
    try:
        return json.loads(s)
    except Exception:
        repaired = _repair_json_text(s)
        if not repaired:
            raise
        return json.loads(repaired)


def normalize_decision_details(parsed: Dict[str, Any]) -> Dict[str, Any]:
    # Aceita payload no schema direto ou encapsulado em caseData.decisionDetails
    if not isinstance(parsed, dict):
        raise ValueError("JSON parseado nao e um objeto.")

    if "caseData" in parsed and isinstance(parsed.get("caseData"), dict):
        case_data = parsed.get("caseData") or {}
        details = case_data.get("decisionDetails")
        if isinstance(details, dict):
            return details

    return parsed


# =============================================================================
# 5) PERSISTENCE
# =============================================================================

def _update_processing_success(
    collection: Collection,
    doc_id: Any,
    model: str,
    latency_ms: int,
    details: Dict[str, Any],
) -> None:
    # Persistencia seguindo padrao do projeto (novo status para decisionDetails)
    now = utc_now()
    update = {
        "$set": {
            "caseData.decisionDetails": details,
            "processing.caseDecisionDetailsStatus": "success",
            "processing.caseDecisionDetailsError": None,
            "processing.caseDecisionDetailsAt": now,
            "processing.caseDecisionDetailsProvider": PROVIDER_NAME,
            "processing.caseDecisionDetailsModel": model,
            "processing.caseDecisionDetailsLatencyMs": latency_ms,
            "processing.pipelineStatus": "decisionDetailsExtracted",
            "status.pipelineStatus": "decisionDetailsExtracted",
            "audit.updatedAt": now,
        }
    }
    collection.update_one({"_id": doc_id}, update)


def _update_processing_error(collection: Collection, doc_id: Any, model: str, error_msg: str) -> None:
    # Persistencia de erro seguindo padrao do projeto
    now = utc_now()
    update = {
        "$set": {
            "processing.caseDecisionDetailsStatus": "error",
            "processing.caseDecisionDetailsError": error_msg,
            "processing.caseDecisionDetailsAt": now,
            "processing.caseDecisionDetailsProvider": PROVIDER_NAME,
            "processing.caseDecisionDetailsModel": model,
            "processing.pipelineStatus": "decisionDetailsExtractError",
            "status.pipelineStatus": "decisionDetailsExtractError",
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
    parser = argparse.ArgumentParser(description="Extrai detalhes de decisoes STF via Mistral")
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
    decision_text = ""
    if isinstance(md, dict):
        decision_text = md.get("decision") or ""

    if decision_text:
        log("Campo caseContent.md.decision encontrado. Conteudo:")
        print(decision_text)
    else:
        log("Campo caseContent.md.decision nao encontrado na decisao. Encerrando.")
        return 0

    # Montar prompt
    log(f"Preparando prompt '{PROMPT_ID}'")
    variables = {var: decision_text for var in prompt_cfg.template_variables}
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

    # Parsear JSON
    log("Processando resposta para gerar JSON estruturado")
    try:
        parsed = parse_json_with_repair(content)
        details = normalize_decision_details(parsed)
    except Exception as e:
        log(f"Erro ao parsear resposta: {e}")
        if doc_id is not None:
            _update_processing_error(collection, doc_id, provider_cfg.model, f"parse_error: {e}")
        return 1

    # Persistir no MongoDB
    log("Salvando caseData.decisionDetails no MongoDB")
    _update_processing_success(collection, doc_id, provider_cfg.model, latency_ms, details)

    log("FINALIZADO COM SUCESSO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
