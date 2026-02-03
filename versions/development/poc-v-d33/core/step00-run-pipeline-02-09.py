#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
-----------------------------------------------------------------------------------------------------
Project: CITO                File: step00-run-pipeline-02-09.py
Version: poc-v-d33      Date: 2026-02-01 (data de criacao/versionamento)
Author:  Codex
-----------------------------------------------------------------------------------------------------
Description: Orchestrates steps 02-09 for a single STF decision or all with processing.pipelineStatus=extracted.
Inputs: config/mongo.yaml, case_data records, optional case URL override.
Outputs: Executes steps 02-09 and relies on each step to persist results.
Pipeline: get case HTML -> clean HTML -> extract sections -> parties/keywords -> legislation -> notes -> doctrine -> decision details.
Dependencies: pymongo
-----------------------------------------------------------------------------------------------------
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml
from utils.mongo import get_case_data_collection


BASE_DIR = Path(__file__).resolve().parent
CONFIG_DIR = BASE_DIR.parent / "config"
MONGO_CONFIG_PATH = CONFIG_DIR / "mongo.yaml"
PIPELINE_CONFIG_PATH = CONFIG_DIR / "pipeline.yaml"

# Delay padrão (fallback)
DELAY_BETWEEN_ITEMS_SECONDS = 10.0
DELAY_BETWEEN_STEPS_SECONDS = 2.0

# Log em arquivo (append)
LOG_DIR = BASE_DIR / "logs"
LOG_FILE_PATH = LOG_DIR / "pipeline-02-09.log"
LOG_TO_FILE = True


# =============================================================================
# 0) LOG
# =============================================================================

def _ts() -> str:
    return datetime.now().strftime("%y-%m-%d %H:%M:%S")


def _write_log_file(text: str) -> None:
    if not LOG_TO_FILE:
        return
    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        with LOG_FILE_PATH.open("a", encoding="utf-8") as f:
            f.write(text)
    except Exception:
        # Evita quebrar o pipeline caso falhe escrita em disco
        pass


def log(msg: str) -> None:
    line = f"[{_ts()}] - {msg}"
    print(line)
    _write_log_file(line + "\n")


def log_block(block: str) -> None:
    print(block)
    if not block.endswith("\n"):
        block += "\n"
    _write_log_file(block)


def _format_duration(seconds: float) -> str:
    total = max(0, int(round(seconds)))
    hours = total // 3600
    minutes = (total % 3600) // 60
    secs = total % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def _box_line(content: str, width: int = 47) -> str:
    trimmed = content[:width]
    return f"│{trimmed:<{width}}│▒"


def _render_box(lines: List[str], width: int = 47, with_separator: bool = False) -> str:
    top = f"┌{'─' * width}┐"
    bottom = f"└{'─' * width}┘▒"
    shadow = f" {'▒' * width}"
    rendered: List[str] = [top]
    if lines:
        if with_separator:
            rendered.append(_box_line(lines[0], width))
            rendered.append(f"╞{'═' * width}╡▒")
            for line in lines[1:]:
                rendered.append(_box_line(line, width))
        else:
            for line in lines:
                rendered.append(_box_line(line, width))
    rendered.append(bottom)
    rendered.append(shadow)
    return "\n".join(rendered)


def _script_title(script: str) -> str:
    mapping = {
        "step01-extract-cases.py": "EXTRACT CASES",
        "step02-get-case-html.py": "GET CASE HTML",
        "step03-clean-case-html.py": "CLEAN HTML",
        "step04-extract-sessions.py": "EXTRACT SECTIONS",
        "step05-extract-keywords-parties.py": "EXTRACT PARTIES AND KEYWORDS",
        "step06-extract-legislation-mistral.py": "EXTRACT LEGISLATION",
        "step07-extract-notes-mistral.py": "EXTRACT NOTES",
        "step08-doctrine-mistral.py": "EXTRACT DOCTRINE",
        "step09-extract-decision-details-mistral.py": "EXTRACT DECISION DETAILS",
    }
    if script in mapping:
        return mapping[script]
    name = re.sub(r"^step\\d+-", "", script).replace(".py", "")
    return name.replace("-", " ").upper().strip()


def _build_startup_banner(
    pipeline_name: str,
    steps: List[tuple[str, str]],
    total: int,
    delay_between_steps: float,
    delay_between_items: float,
) -> str:
    header = _render_box([f"  {pipeline_name}"], with_separator=False)
    workflow_lines = ["  PIPELINE WORKFLOW"]
    for idx, (script, _) in enumerate(steps, start=1):
        workflow_lines.append(f"  {idx}. {_script_title(script)}")
    workflow = _render_box(workflow_lines, with_separator=True)

    estimated_seconds = (len(steps) * total * delay_between_steps) + max(total - 1, 0) * delay_between_items
    execution_lines = [
        "  PIPELINE EXECUTION STARTED",
        f"  CASES TO PROCESS: {total}",
        f"  ESTIMATED TIME: {_format_duration(estimated_seconds)}",
        f"  DATETIME: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
    ]
    execution = _render_box(execution_lines, with_separator=True)
    return "\n\n".join([header, workflow, execution]).strip()


def _build_summary_banner(total: int, status_counts: Dict[str, int]) -> str:
    lines = ["  PIPELINE EXECUTION SUMMARY", f"  TOTAL CASES: {total}"]
    for status, count in sorted(status_counts.items()):
        lines.append(f"  {status.upper()}: {count}")
    return _render_box(lines, with_separator=True)


# =============================================================================
# 1) MONGO
# =============================================================================

def _get_case_data_collection():
    # Conecta ao MongoDB usando config/mongo.yaml e retorna a collection case_data.
    return get_case_data_collection(MONGO_CONFIG_PATH, "case_data")


# =============================================================================
# 2) SUBPROCESS
# =============================================================================

def _run_step(script: str, input_text: str) -> int:
    # Executa o step como subprocesso, injetando o input necessário via stdin.
    cmd = [sys.executable, str(BASE_DIR / script)]
    result = subprocess.run(
        cmd,
        input=input_text,
        text=True,
        cwd=str(BASE_DIR),
    )
    return result.returncode


# =============================================================================
# 3) PROMPT
# =============================================================================

def _prompt_mode() -> tuple[str, Optional[str]]:
    """
    Pergunta ao usuario o modo de execucao.
    Retorna (mode, stf_decision_id).
    mode in {"all", "one"}
    """
    print("\nSelecione o modo de execucao:")
    print("  1) Processar TODOS com processing.pipelineStatus = extracted")
    print("  2) Processar APENAS um documento por stfDecisionId")

    while True:
        choice = input("Opcao (1/2): ").strip()
        if choice == "1":
            return "all", None
        if choice == "2":
            stf_decision_id = input("Informe o stfDecisionId: ").strip()
            if stf_decision_id:
                return "one", stf_decision_id
            print("stfDecisionId vazio. Tente novamente.")
        else:
            print("Opcao invalida. Use 1 ou 2.")


def _load_yaml(path: Path) -> Dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(f"Config nao encontrado: {path.resolve()}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def _get_pipeline_cfg(raw: Dict[str, Any]) -> Dict[str, Any]:
    return raw.get("pipeline") if isinstance(raw.get("pipeline"), dict) else {}


# =============================================================================
# 4) MAIN
# =============================================================================

def main() -> int:
    parser = argparse.ArgumentParser(description="Executa o pipeline (steps 02-09) por stfDecisionId.")
    parser.add_argument(
        "--case-url",
        help="caseContent.caseUrl (use se o step02 nao encontrar URL no MongoDB).",
    )
    args = parser.parse_args()

    # Carregar pipeline.yaml
    try:
        pipeline_raw = _load_yaml(PIPELINE_CONFIG_PATH)
        pipeline_cfg = _get_pipeline_cfg(pipeline_raw)
    except Exception as e:
        log(f"Erro ao carregar pipeline.yaml: {e}")
        return 1

    runtime_cfg = pipeline_cfg.get("runtime") if isinstance(pipeline_cfg.get("runtime"), dict) else {}
    execution_cfg = pipeline_cfg.get("execution") if isinstance(pipeline_cfg.get("execution"), dict) else {}
    mode_cfg = pipeline_cfg.get("mode") if isinstance(pipeline_cfg.get("mode"), dict) else {}
    logging_cfg = pipeline_cfg.get("logging") if isinstance(pipeline_cfg.get("logging"), dict) else {}

    # Ajusta logging conforme config
    global LOG_DIR, LOG_FILE_PATH, LOG_TO_FILE
    LOG_TO_FILE = logging_cfg.get("enabled") is not False
    if isinstance(logging_cfg.get("log_dir"), str) and logging_cfg.get("log_dir"):
        LOG_DIR = Path(str(logging_cfg.get("log_dir"))).expanduser()
    if isinstance(logging_cfg.get("log_file"), str) and logging_cfg.get("log_file"):
        LOG_FILE_PATH = LOG_DIR / str(logging_cfg.get("log_file"))

    # case_url override
    case_url = (args.case_url or "").strip() or str(runtime_cfg.get("case_url_override") or "").strip()

    log("Conectando ao MongoDB")
    try:
        col = _get_case_data_collection()
    except Exception as e:
        log(f"Falha ao conectar no MongoDB: {e}")
        return 1

    run_mode = str(mode_cfg.get("run_mode") or "").strip()
    stf_decision_id_cfg = str(mode_cfg.get("stf_decision_id") or "").strip() if mode_cfg.get("stf_decision_id") else None

    if run_mode not in {"one", "all"}:
        mode, stf_decision_id = _prompt_mode()
    else:
        mode = run_mode
        stf_decision_id = stf_decision_id_cfg

    if mode == "one":
        stf_ids = [stf_decision_id] if stf_decision_id else []
    else:
        filter_cfg = mode_cfg.get("filter") if isinstance(mode_cfg.get("filter"), dict) else {}
        pipeline_status = str(filter_cfg.get("processing_pipeline_status") or "extracted")
        query = {"processing.pipelineStatus": pipeline_status}
        projection = {"identity.stfDecisionId": 1}
        log(f"Buscando documentos com processing.pipelineStatus = {pipeline_status}")
        docs = list(col.find(query, projection=projection))
        stf_ids = [
            ((doc.get("identity") or {}).get("stfDecisionId") or "").strip()
            for doc in docs
            if ((doc.get("identity") or {}).get("stfDecisionId") or "").strip()
        ]

    if not stf_ids:
        log("Nenhum stfDecisionId encontrado para processar.")
        return 0

    total = len(stf_ids)
    log(f"Total encontrado: {total}")

    steps_cfg = execution_cfg.get("steps") if isinstance(execution_cfg.get("steps"), list) else []
    step_defs: List[tuple[str, str]] = []
    for s in steps_cfg:
        if not isinstance(s, dict):
            continue
        if s.get("enabled") is False:
            continue
        script = str(s.get("script") or "").strip()
        if not script:
            continue
        fmt = str(s.get("input_format") or "{stfDecisionId}\\n")
        step_defs.append((script, fmt))

    delay_steps = float(execution_cfg.get("delay_between_steps_seconds") or DELAY_BETWEEN_STEPS_SECONDS)
    delay_items = float(execution_cfg.get("delay_between_items_seconds") or DELAY_BETWEEN_ITEMS_SECONDS)
    startup_banner = _build_startup_banner(
        "CITO poc v-D33",
        step_defs,
        total,
        delay_steps,
        delay_items,
    )
    log_block(startup_banner)

    status_counts: Dict[str, int] = {"success": 0, "failed": 0}

    for idx, stf_id in enumerate(stf_ids, start=1):
        log(f"({idx}/{total}) Processando stfDecisionId={stf_id}")
        try:
            doc = col.find_one(
                {"identity.stfDecisionId": stf_id},
                projection={
                    "identity.stfDecisionId": 1,
                    "identity.caseClass": 1,
                    "identity.caseNumber": 1,
                    "identity.caseTitle": 1,
                    "identity.judgingBody": 1,
                    "identity.rapporteur": 1,
                    "dates.judgmentDate": 1,
                    "dates.publicationDate": 1,
                    "caseTitle": 1,
                    "status.pipelineStatus": 1,
                },
            )
            if doc:
                identity = doc.get("identity") or {}
                dates = doc.get("dates") or {}
                log(
                    "Identificacao | "
                    f"stfDecisionId={identity.get('stfDecisionId')} | "
                    f"caseClass={identity.get('caseClass')} | "
                    f"caseNumber={identity.get('caseNumber')} | "
                    f"caseTitle={identity.get('caseTitle') or doc.get('caseTitle')} | "
                    f"judgingBody={identity.get('judgingBody')} | "
                    f"rapporteur={identity.get('rapporteur')} | "
                    f"judgmentDate={dates.get('judgmentDate')} | "
                    f"publicationDate={dates.get('publicationDate')} | "
                    f"pipelineStatus={(doc.get('status') or {}).get('pipelineStatus')}"
                )
            else:
                log("Identificacao | Documento nao encontrado para stfDecisionId informado.")
        except Exception as e:
            log(f"Aviso: falha ao buscar identificacao do processo: {e}")

        steps: List[tuple[str, str]] = []
        for script, fmt in step_defs:
            input_text = fmt.format(stfDecisionId=stf_id, caseUrl=case_url or "")
            steps.append((script, input_text))

        case_failed = False
        for script, input_text in steps:
            log(f"Executando: {script}")
            rc = _run_step(script, input_text)
            if rc != 0:
                log(f"ERRO: {script} retornou codigo {rc}")
                case_failed = True
                if execution_cfg.get("stop_on_error") is True:
                    return 1
            else:
                log(f"OK: {script}")
            if delay_steps > 0:
                time.sleep(delay_steps)
        if idx < total and delay_items > 0:
            log(f"Aguardando {delay_items}s antes do proximo item...")
            time.sleep(delay_items)

        if case_failed:
            status_counts["failed"] += 1
        else:
            status_counts["success"] += 1

    log("Pipeline finalizado.")
    log_block(_build_summary_banner(total, status_counts))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
