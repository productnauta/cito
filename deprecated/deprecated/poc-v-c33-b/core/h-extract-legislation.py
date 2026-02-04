#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
h_extract_legislation_references.py

Pipeline: Extract Legislation References (Projeto CITO)

Objective:
- Read case_data by identity.stfDecisionId
- Input: caseContent.md.legislation (PT-BR, unstructured)
- Output: caseData.legislationReferences (structured JSON)
- Metadata: processing.caseLegislationRefs*
- Status on success OR empty input: status.pipelineStatus = "legislationExtracted"

Rules:
- Use MongoDB connection parameters from config/mongo.json
- Load AI model configuration from config/ai-model.json
- Structured logs for each step
- Idempotent and atomic updates
- Do not invoke AI if input empty/blank
- If AI output invalid/error: set processing.caseLegislationRefsStatus="error" and DO NOT update status.pipelineStatus

Provider:
- Only Groq is supported in this script.
  The config/ai-model.json must include a "groq" provider entry.

Dependencies:
  pip install pymongo requests
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, Optional, Tuple

import requests
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError


# ==============================================================================
# Logging (structured)
# ==============================================================================

def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def log(level: str, event: str, **fields: Any) -> None:
    payload = {
        "ts": utc_now_iso(),
        "level": level.upper(),
        "event": event,
        **fields,
    }
    print(json.dumps(payload, ensure_ascii=False))


# ==============================================================================
# Config loaders
# ==============================================================================

def _read_json_file(path: str) -> Dict[str, Any]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"Config file not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_mongo_config(path: str = "config/mongo.json") -> Dict[str, Any]:
    """
    Expected (flexible) keys (any of):
      - mongo_uri / uri / MONGO_URI
      - db_name / database / DB_NAME
      - collection_case_data / collection / CASE_DATA_COLLECTION
    """
    raw = _read_json_file(path)

    def pick(*keys: str) -> Optional[Any]:
        for k in keys:
            if k in raw and raw[k] not in (None, ""):
                return raw[k]
        return None

    mongo_uri = pick("mongo_uri", "uri", "MONGO_URI") or os.getenv("MONGO_URI")
    db_name = pick("db_name", "database", "DB_NAME") or os.getenv("DB_NAME")
    collection = (
        pick("collection_case_data", "collection", "CASE_DATA_COLLECTION")
        or os.getenv("CASE_DATA_COLLECTION")
        or "case_data"
    )

    if not mongo_uri:
        raise ValueError("MongoDB URI not found (config/mongo.json or env MONGO_URI).")
    if not db_name:
        raise ValueError("MongoDB DB name not found (config/mongo.json or env DB_NAME).")

    return {"mongo_uri": mongo_uri, "db_name": db_name, "collection_case_data": collection}


def load_ai_model_config(provider: str = "groq", path: str = "config/ai-model.json") -> dict:
    """
    Loads ONLY the 'groq' provider configuration, per requirement.
    """
    with open(path, "r", encoding="utf-8") as f:
        cfg = json.load(f)

    if provider not in cfg:
        raise ValueError(f"AI provider '{provider}' not found in ai-model.json")

    raw = cfg[provider]

    def env_or_default(env_key, default=None):
        return os.getenv(env_key) if env_key and os.getenv(env_key) else default

    resolved = {
        "provider": raw["provider"],
        "api_key": raw.get("api_key") or env_or_default(raw.get("api_key_env")),
        "api_base": env_or_default(raw.get("api_base_env"), raw.get("api_base_default")),
        "model": env_or_default(raw.get("model_env"), raw.get("model_default", raw.get("model"))),
        "request_timeout_seconds": raw["request_timeout_seconds"],
        "retries": raw["retries"],
        "api_delay_seconds": raw.get("api_delay_seconds", 0),
    }

    missing = [k for k in ("provider", "api_key", "model", "request_timeout_seconds", "retries") if not resolved.get(k)]
    if missing:
        raise ValueError(f"AI model runtime config missing fields: {missing}")

    return resolved


# ==============================================================================
# Mongo helpers (atomic + idempotent)
# ==============================================================================

def get_collection(mongo_cfg: Dict[str, Any]) -> Collection:
    client = MongoClient(mongo_cfg["mongo_uri"])
    db = client[mongo_cfg["db_name"]]
    return db[mongo_cfg["collection_case_data"]]


def find_case_by_stf_id(col: Collection, stf_decision_id: str) -> Optional[Dict[str, Any]]:
    return col.find_one({"identity.stfDecisionId": stf_decision_id})


def atomic_update_processing(
    col: Collection,
    doc_id: Any,
    *,
    processing_patch: Dict[str, Any],
    set_pipeline_status: bool,
) -> None:
    """
    Single atomic update.
    If set_pipeline_status=True, writes status.pipelineStatus="legislationExtracted".
    """
    update_doc: Dict[str, Any] = {"$set": {}}

    # always write processing fields
    update_doc["$set"].update(processing_patch)

    if set_pipeline_status:
        update_doc["$set"]["status.pipelineStatus"] = "legislationExtracted"

    res = col.update_one({"_id": doc_id}, update_doc)
    if res.matched_count != 1:
        raise RuntimeError("Atomic update failed (document not matched).")


# ==============================================================================
# Schema validation (strict enough to protect pipeline)
# ==============================================================================

JURISDICTION_ENUM = {"federal", "state", "municipal", "unknown"}
NORMTYPE_ENUM = {"CF", "EC", "LC", "LEI", "DECRETO", "RESOLUÇÃO", "PORTARIA", "OUTRA"}


def _is_int_or_none(v: Any) -> bool:
    return v is None or isinstance(v, int)


def _is_bool(v: Any) -> bool:
    return isinstance(v, bool)


def validate_legislation_schema(payload: Any) -> Tuple[bool, str]:
    """
    Validates:
      {"caseData":{"legislationReferences":[{...}]}}
    """
    if not isinstance(payload, dict):
        return False, "root must be an object"

    case_data = payload.get("caseData")
    if not isinstance(case_data, dict):
        return False, "caseData must be an object"

    refs = case_data.get("legislationReferences")
    if not isinstance(refs, list):
        return False, "caseData.legislationReferences must be an array"

    for i, item in enumerate(refs):
        if not isinstance(item, dict):
            return False, f"legislationReferences[{i}] must be an object"

        jl = item.get("jurisdictionLevel")
        if jl not in JURISDICTION_ENUM:
            return False, f"legislationReferences[{i}].jurisdictionLevel invalid"

        nt = item.get("normType")
        if nt not in NORMTYPE_ENUM:
            return False, f"legislationReferences[{i}].normType invalid"

        ni = item.get("normIdentifier")
        if not isinstance(ni, str) or not ni.strip():
            return False, f"legislationReferences[{i}].normIdentifier must be non-empty string"

        ny = item.get("normYear")
        if not (ny is None or (isinstance(ny, str) and re.fullmatch(r"\d{4}", ny))):
            return False, f"legislationReferences[{i}].normYear must be YYYY or null"

        nd = item.get("normDescription")
        if not isinstance(nd, str):
            return False, f"legislationReferences[{i}].normDescription must be string"

        nrefs = item.get("normReferences")
        if not isinstance(nrefs, list):
            return False, f"legislationReferences[{i}].normReferences must be an array"

        for j, dev in enumerate(nrefs):
            if not isinstance(dev, dict):
                return False, f"normReferences[{j}] must be an object"

            if not _is_int_or_none(dev.get("articleNumber")):
                return False, f"normReferences[{j}].articleNumber must be int|null"
            if not _is_bool(dev.get("isCaput")):
                return False, f"normReferences[{j}].isCaput must be boolean"
            if not _is_int_or_none(dev.get("incisoNumber")):
                return False, f"normReferences[{j}].incisoNumber must be int|null"
            if not _is_int_or_none(dev.get("paragraphNumber")):
                return False, f"normReferences[{j}].paragraphNumber must be int|null"
            if not _is_bool(dev.get("isParagraphSingle")):
                return False, f"normReferences[{j}].isParagraphSingle must be boolean"

            lc = dev.get("letterCode")
            if not (lc is None or isinstance(lc, str)):
                return False, f"normReferences[{j}].letterCode must be string|null"

    return True, "ok"


# ==============================================================================
# AI prompt + Groq calls
# ==============================================================================

PROMPT_NAME = "Extração Legislativa CITO"

PROMPT_INSTRUCTIONS = r"""
Extract legislative references from the provided text and return only a JSON strictly matching the defined schema.

Schema (strict):
{"caseData":{"legislationReferences":[{"jurisdictionLevel":"federal|state|municipal|unknown","normType":"CF|EC|LC|LEI|DECRETO|RESOLUÇÃO|PORTARIA|OUTRA","normIdentifier":"TIPO-NUM-ANO","normYear":"YYYY|null","normDescription":"string","normReferences":[{"articleNumber":int|null,"isCaput":bool,"incisoNumber":int|null,"paragraphNumber":int|null,"isParagraphSingle":bool,"letterCode":"string|null"}]}]}}

Normalization Rules:
- Remove leading zeros (e.g., ART-00022 -> 22).
- Normalize identifiers to TIPO-NUM-ANO.
- Devices on the same line or sequential lines inherit the last declared articleNumber.
- Flags:
  - CAPUT -> isCaput = true
  - PAR-ÚNICO -> isParagraphSingle = true
- Jurisdiction inference:
  - Federal: CF, LC, LEG-FED
  - State: LEG-EST, state acronyms
  - Municipal: explicit municipal indicators
- Missing data must be set to null. No implicit inference.

Return ONLY valid JSON. No markdown. No commentary.
""".strip()


def extract_json_from_text(text: str) -> Any:
    """
    Accepts either:
      - pure JSON
      - JSON wrapped with leading/trailing text
    Strategy:
      - find first '{' and last '}' and attempt parse
    """
    s = (text or "").strip()
    if not s:
        raise ValueError("empty AI response")

    # remove common fenced code blocks
    s = re.sub(r"^```(?:json)?\s*", "", s, flags=re.IGNORECASE).strip()
    s = re.sub(r"\s*```$", "", s).strip()

    first = s.find("{")
    last = s.rfind("}")
    if first == -1 or last == -1 or last <= first:
        raise ValueError("no JSON object delimiters found")

    candidate = s[first:last + 1]
    return json.loads(candidate)


@dataclass(frozen=True)
class AiRuntimeConfig:
    provider: str
    api_key: str
    api_base: Optional[str]
    model: str
    request_timeout_seconds: int
    retries: int
    api_delay_seconds: float


def to_ai_runtime(cfg: Dict[str, Any]) -> AiRuntimeConfig:
    return AiRuntimeConfig(
        provider=str(cfg["provider"]).strip().lower(),
        api_key=str(cfg["api_key"]).strip(),
        api_base=(str(cfg["api_base"]).strip() if cfg.get("api_base") else None),
        model=str(cfg["model"]).strip(),
        request_timeout_seconds=int(cfg["request_timeout_seconds"]),
        retries=int(cfg["retries"]),
        api_delay_seconds=float(cfg.get("api_delay_seconds") or 0),
    )


def call_ai_extract_legislation(ai: AiRuntimeConfig, legislation_text: str) -> Dict[str, Any]:
    if ai.provider != "groq":
        raise ValueError("This script supports only provider='groq'.")

    raw = call_groq(ai, legislation_text)
    parsed = extract_json_from_text(raw)

    ok, reason = validate_legislation_schema(parsed)
    if not ok:
        raise ValueError(f"invalid schema: {reason}")

    return parsed


def _request_with_retries(
    *,
    method: str,
    url: str,
    headers: Dict[str, str],
    json_body: Dict[str, Any],
    timeout_s: int,
    retries: int,
    api_delay_s: float,
) -> str:
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        if api_delay_s and attempt > 1:
            time.sleep(api_delay_s)

        try:
            resp = requests.request(
                method=method,
                url=url,
                headers=headers,
                json=json_body,
                timeout=timeout_s,
            )
            if resp.status_code >= 400:
                raise RuntimeError(f"HTTP {resp.status_code}: {resp.text[:4000]}")
            return resp.text
        except Exception as e:
            last_err = e
            log("WARN", "ai_http_attempt_failed", attempt=attempt, retries=retries, error=str(e))
            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 8))
                continue
            break
    raise RuntimeError(f"AI request failed after {retries} attempts: {last_err}")


def call_groq(ai: AiRuntimeConfig, legislation_text: str) -> str:
    """
    Groq Chat Completions endpoint:
      POST {api_base}/chat/completions
    Default api_base: https://api.groq.com/openai/v1
    """
    api_base = ai.api_base or "https://api.groq.com/openai/v1"
    url = api_base.rstrip("/") + "/chat/completions"
    headers = {
        "Authorization": f"Bearer {ai.api_key}",
        "Content-Type": "application/json",
    }
    body = {
        "model": ai.model,
        "temperature": 0,
        "messages": [
            {"role": "system", "content": f"{PROMPT_NAME}\n\n{PROMPT_INSTRUCTIONS}"},
            {"role": "user", "content": legislation_text},
        ],
    }

    raw = _request_with_retries(
        method="POST",
        url=url,
        headers=headers,
        json_body=body,
        timeout_s=ai.request_timeout_seconds,
        retries=ai.retries,
        api_delay_s=ai.api_delay_seconds,
    )

    data = json.loads(raw)
    return data["choices"][0]["message"]["content"]


# ==============================================================================
# Main pipeline
# ==============================================================================

def is_blank(s: Optional[str]) -> bool:
    return not (s or "").strip()


def main() -> int:
    parser = argparse.ArgumentParser(description="CITO | Extract legislation references via Groq.")
    parser.add_argument("--stfDecisionId", help="identity.stfDecisionId")
    parser.add_argument("--provider", default="groq", help="AI provider key (must be 'groq')")
    parser.add_argument("--mongoConfig", default="config/mongo.json", help="Mongo config JSON path")
    parser.add_argument("--aiConfig", default="config/ai-model.json", help="AI model config JSON path")
    parser.add_argument("--force", action="store_true", help="Reprocess even if status.pipelineStatus already legislationExtracted")
    args = parser.parse_args()

    stf_id = (args.stfDecisionId or "").strip()
    if not stf_id:
        # Requirement: request identity.stfDecisionId
        stf_id = input("Informe identity.stfDecisionId: ").strip()

    if not stf_id:
        log("ERROR", "input_invalid", reason="identity.stfDecisionId vazio")
        return 2

    if (args.provider or "").strip().lower() != "groq":
        log("ERROR", "provider_invalid", provider=args.provider, reason="only groq is supported")
        return 2

    log("INFO", "pipeline_start", stfDecisionId=stf_id, provider="groq")

    # Load configs
    try:
        mongo_cfg = load_mongo_config(args.mongoConfig)
        ai_cfg_raw = load_ai_model_config("groq", args.aiConfig)
        ai_cfg = to_ai_runtime(ai_cfg_raw)

        log(
            "INFO",
            "config_loaded",
            mongo_db=mongo_cfg["db_name"],
            mongo_collection=mongo_cfg["collection_case_data"],
            ai_provider=ai_cfg.provider,
            ai_model=ai_cfg.model,
        )
    except Exception as e:
        log("ERROR", "config_load_failed", error=str(e))
        return 2

    # Mongo get document
    try:
        col = get_collection(mongo_cfg)
        doc = find_case_by_stf_id(col, stf_id)
        if not doc:
            log("ERROR", "case_not_found", stfDecisionId=stf_id)
            return 3
    except PyMongoError as e:
        log("ERROR", "mongo_error", step="find_case", error=str(e))
        return 4

    doc_id = doc["_id"]
    pipeline_status = (((doc.get("status") or {}).get("pipelineStatus")) if isinstance(doc.get("status"), dict) else None)

    if (pipeline_status == "legislationExtracted") and not args.force:
        log("INFO", "already_processed_skip", stfDecisionId=stf_id, docId=str(doc_id))
        return 0

    # Input field
    legislation_text = (
        ((((doc.get("caseContent") or {}).get("md") or {}).get("legislation")))
        if isinstance(doc.get("caseContent"), dict)
        else None
    )
    legislation_text = (legislation_text or "").strip()

    # Empty handling
    if is_blank(legislation_text):
        log("INFO", "empty_input", stfDecisionId=stf_id, docId=str(doc_id))

        processing_patch = {
            "processing.caseLegislationRefsStatus": "empty",
            "processing.caseLegislationRefsError": "legislation vazio",
            "processing.caseLegislationRefsAt": utc_now_iso(),
        }

        try:
            atomic_update_processing(col, doc_id, processing_patch=processing_patch, set_pipeline_status=True)
            log("INFO", "mongo_updated_empty", stfDecisionId=stf_id, docId=str(doc_id), pipelineStatus="legislationExtracted")
            return 0
        except Exception as e:
            log("ERROR", "mongo_update_failed", step="empty_update", error=str(e))
            return 4

    # AI processing
    log("INFO", "ai_call_start", stfDecisionId=stf_id, docId=str(doc_id), text_len=len(legislation_text))

    try:
        started = time.time()
        result_json = call_ai_extract_legislation(ai_cfg, legislation_text)
        elapsed_ms = int((time.time() - started) * 1000)

        # Persist output + metadata + pipeline status (single atomic update)
        processing_patch = {
            "caseData.legislationReferences": result_json["caseData"]["legislationReferences"],
            "processing.caseLegislationRefsStatus": "success",
            "processing.caseLegislationRefsError": None,
            "processing.caseLegislationRefsAt": utc_now_iso(),
            "processing.caseLegislationRefsProvider": ai_cfg.provider,
            "processing.caseLegislationRefsModel": ai_cfg.model,
            "processing.caseLegislationRefsLatencyMs": elapsed_ms,
        }

        atomic_update_processing(col, doc_id, processing_patch=processing_patch, set_pipeline_status=True)

        log(
            "INFO",
            "pipeline_success",
            stfDecisionId=stf_id,
            docId=str(doc_id),
            refs_count=len(result_json["caseData"]["legislationReferences"]),
            latencyMs=elapsed_ms,
            pipelineStatus="legislationExtracted",
        )
        return 0

    except Exception as e:
        # error path: write processing.* but DO NOT update status.pipelineStatus
        err_msg = str(e)
        log("ERROR", "ai_or_validation_failed", stfDecisionId=stf_id, docId=str(doc_id), error=err_msg)

        try:
            processing_patch = {
                "processing.caseLegislationRefsStatus": "error",
                "processing.caseLegislationRefsError": err_msg,
                "processing.caseLegislationRefsAt": utc_now_iso(),
                "processing.caseLegislationRefsProvider": ai_cfg.provider,
                "processing.caseLegislationRefsModel": ai_cfg.model,
            }
            atomic_update_processing(col, doc_id, processing_patch=processing_patch, set_pipeline_status=False)
            log("INFO", "mongo_updated_error", stfDecisionId=stf_id, docId=str(doc_id))
        except Exception as e2:
            log("ERROR", "mongo_update_failed", step="error_update", error=str(e2))

        return 1


if __name__ == "__main__":
    sys.exit(main())
