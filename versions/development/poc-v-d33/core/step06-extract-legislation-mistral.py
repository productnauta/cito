#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step06-extract-legislation-mistral.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Extracts legislation references via Mistral and stores normalized references.
Inputs: config/mongo.yaml, config/providers.yaml, config/prompts.yaml, caseContent.md.legislation.
Outputs: caseData.legislationReferences + processing/status updates.
Pipeline: load config -> query Mongo -> Mistral extraction -> parse line protocol -> persist.
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
PROVIDER_KEY_NAME = "cito-dev-b"
PROMPT_ID = "extract-legislation-from-md"


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


def build_provider_cfg(raw: Dict[str, Any], provider_name: str, key_name: str) -> ProviderCfg:
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

_ALLOWED_JUR = {"federal", "state", "municipal", "unknown"}
_ALLOWED_TYPES = {"CF", "EC", "LC", "LEI", "DECRETO", "RESOLUCAO", "PORTARIA", "OUTRA"}


def _parse_int(value: str) -> Optional[int]:
    s = (value or "").strip()
    if not s:
        return None
    try:
        return int(s)
    except ValueError:
        return None


def _roman_to_int(s: str) -> Optional[int]:
    s = (s or "").strip().upper()
    if not s:
        return None
    roman_map = {"I": 1, "V": 5, "X": 10, "L": 50, "C": 100, "D": 500, "M": 1000}
    if any(ch not in roman_map for ch in s):
        return None
    total = 0
    prev = 0
    for ch in reversed(s):
        val = roman_map[ch]
        if val < prev:
            total -= val
        else:
            total += val
            prev = val
    return total if total > 0 else None


def _normalize_norm_identifier(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return ""
    s = s.replace("/", "-")
    s = s.replace("_", "-")
    s = s.replace("–", "-")
    s = s.replace("—", "-")
    s = s.replace(" ", "")
    parts = s.split("-")
    if len(parts) >= 3:
        typ = parts[0]
        num = parts[1].replace(".", "")
        year = parts[2]
        return f"{typ}-{num}-{year}"
    return s


def _normalize_jurisdiction(raw: str) -> str:
    s = (raw or "").strip().lower()
    return s if s in _ALLOWED_JUR else "unknown"


def _normalize_norm_type(raw: str) -> str:
    s = (raw or "").strip().upper()
    return s if s in _ALLOWED_TYPES else "OUTRA"


def parse_line_protocol(text: str) -> List[Dict[str, Any]]:
    lines = [ln.strip() for ln in (text or "").splitlines() if ln.strip()]
    norms: Dict[str, Dict[str, Any]] = {}
    current_norm_id: Optional[str] = None

    for line in lines:
        parts = [p.strip() for p in line.split("|")]
        if not parts:
            continue
        tag = parts[0].upper()

        if tag == "N":
            norm_id_raw = parts[1] if len(parts) > 1 else ""
            jur_raw = parts[2] if len(parts) > 2 else ""
            tipo_raw = parts[3] if len(parts) > 3 else ""
            ano_raw = parts[4] if len(parts) > 4 else ""
            desc_raw = parts[5] if len(parts) > 5 else ""

            norm_id = _normalize_norm_identifier(norm_id_raw)
            if not norm_id:
                current_norm_id = None
                continue

            norm_year = _parse_int(ano_raw)
            if norm_year == 0:
                norm_year = None

            if norm_id not in norms:
                norms[norm_id] = {
                    "normIdentifier": norm_id,
                    "jurisdictionLevel": _normalize_jurisdiction(jur_raw),
                    "normType": _normalize_norm_type(tipo_raw),
                    "normYear": norm_year,
                    "normDescription": (desc_raw or "").strip(),
                    "normReferences": [],
                    "_ref_set": set(),
                }
            current_norm_id = norm_id
            continue

        if tag == "R":
            if not current_norm_id or current_norm_id not in norms:
                continue

            art_raw = parts[1] if len(parts) > 1 else ""
            caput_raw = parts[2] if len(parts) > 2 else ""
            inc_raw = parts[3] if len(parts) > 3 else ""
            par_raw = parts[4] if len(parts) > 4 else ""
            par_unico_raw = parts[5] if len(parts) > 5 else ""
            letra_raw = parts[6] if len(parts) > 6 else ""

            article_number = _parse_int(art_raw)
            if article_number is None:
                continue

            inciso_number = _parse_int(inc_raw)
            if inciso_number is None:
                inciso_number = _roman_to_int(inc_raw)

            paragraph_number = _parse_int(par_raw)
            is_paragraph_single = (par_unico_raw or "").strip() == "1"
            letter_code = (letra_raw or "").strip().lower() or None

            is_caput = (caput_raw or "").strip() == "1"
            if not is_caput and inciso_number is None and paragraph_number is None and not is_paragraph_single and not letter_code:
                is_caput = True

            ref = {
                "articleNumber": article_number,
                "isCaput": bool(is_caput),
                "incisoNumber": inciso_number,
                "paragraphNumber": paragraph_number,
                "isParagraphSingle": bool(is_paragraph_single),
                "letterCode": letter_code,
            }

            ref_key = (
                ref["articleNumber"],
                ref["isCaput"],
                ref["incisoNumber"],
                ref["paragraphNumber"],
                ref["isParagraphSingle"],
                ref["letterCode"],
            )

            ref_set = norms[current_norm_id]["_ref_set"]
            if ref_key not in ref_set:
                ref_set.add(ref_key)
                norms[current_norm_id]["normReferences"].append(ref)

    # Remover set auxiliar
    output = []
    for norm in norms.values():
        norm.pop("_ref_set", None)
        output.append(norm)

    return output


# =============================================================================
# 5) PIPELINE
# =============================================================================


def _update_processing_success(collection: Collection, doc_id: Any, model: str, latency_ms: int, refs: List[Dict[str, Any]]) -> None:
    now = utc_now()
    update = {
        "$set": {
            "caseData.legislationReferences": refs,
            "processing.caseLegislationRefsStatus": "success",
            "processing.caseLegislationRefsError": None,
            "processing.caseLegislationRefsAt": now,
            "processing.caseLegislationRefsProvider": PROVIDER_NAME,
            "processing.caseLegislationRefsModel": model,
            "processing.caseLegislationRefsLatencyMs": latency_ms,
            "processing.pipelineStatus": "legislationExtracted",
            "status.pipelineStatus": "legislationExtracted",
            "audit.updatedAt": now,
            "status.updatedAt": now,
        }
    }
    collection.update_one({"_id": doc_id}, update)


def _update_processing_error(collection: Collection, doc_id: Any, model: str, error_msg: str) -> None:
    now = utc_now()
    update = {
        "$set": {
            "processing.caseLegislationRefsStatus": "error",
            "processing.caseLegislationRefsError": error_msg,
            "processing.caseLegislationRefsAt": now,
            "processing.caseLegislationRefsProvider": PROVIDER_NAME,
            "processing.caseLegislationRefsModel": model,
            "processing.pipelineStatus": "legislationExtractError",
            "status.pipelineStatus": "legislationExtractError",
            "audit.updatedAt": now,
            "status.updatedAt": now,
        }
    }
    collection.update_one({"_id": doc_id}, update)


def main() -> int:
    log("INICIANDO EXECUCAO DO SCRIPT")

    parser = argparse.ArgumentParser(description="Extrai referencias legislativas via Mistral")
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
        provider_cfg = build_provider_cfg(load_yaml(PROVIDERS_CONFIG_PATH), PROVIDER_NAME, PROVIDER_KEY_NAME)
        prompt_cfg = build_prompt_cfg(load_yaml(PROMPTS_CONFIG_PATH), PROMPT_ID)
        log(
            "IA em uso | "
            f"PROVIDER={PROVIDER_NAME} | PROVIDER_KEY_NAME={PROVIDER_KEY_NAME} | "
            f"MODEL={provider_cfg.model} | Temperature={provider_cfg.temperature}"
        )
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
    legislation_text = ""
    if isinstance(md, dict):
        legislation_text = md.get("legislation") or ""

    if legislation_text:
        log("Campo caseContent.md.legislation encontrado. Conteudo:")
        print(legislation_text)
    else:
        log("Campo caseContent.md.legislation nao encontrado na decisao. Encerrando.")
        return 0

    # Montar prompt
    log(f"Preparando prompt '{PROMPT_ID}'")
    variables = {var: legislation_text for var in prompt_cfg.template_variables}
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
        refs = parse_line_protocol(content)
    except Exception as e:
        log(f"Erro ao parsear resposta: {e}")
        if doc_id is not None:
            _update_processing_error(collection, doc_id, provider_cfg.model, f"parse_error: {e}")
        return 1

    # Persistir no MongoDB
    log("Salvando caseData.legislationReferences no MongoDB")
    _update_processing_success(collection, doc_id, provider_cfg.model, latency_ms, refs)

    log("FINALIZADO COM SUCESSO")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
