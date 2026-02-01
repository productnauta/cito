from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Set, Tuple

import pandas as pd
import gspread
from google.oauth2.service_account import Credentials


# =============================================================================
# 1) CONSTANTES / DEFAULTS
# =============================================================================

SPREADSHEET_URL: str = (
    "https://docs.google.com/spreadsheets/d/"
    "1XvNJWRsAyoasc6IE9v6-uG_o8vi_7OkS2hvWaL281MA/edit?usp=sharing"
)
WORKSHEET_NAME: str = "config"
SERVICE_ACCOUNT_FILE: Path = Path("poc/v-a33-240125/config/service_account.json")

STATUS_ALIASES: Tuple[str, ...] = ("status", "stauts")
FILTER_STATUSES: Set[str] = {"active"}

REQUIRED_COLUMNS: Tuple[str, ...] = ("id", "status", "config_name", "value")

# Se existir duplicidade de config_name, a escolha do "último" deve ser determinística.
# Ordenar por id e (opcionalmente) por config_name torna isso previsível.
SORT_FOR_DEDUP: Tuple[str, ...] = ("id", "config_name")


# =============================================================================
# 2) LOG
# =============================================================================

def log(level: str, message: str) -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{level}] {message}")


# =============================================================================
# 3) MODELO TIPADO DE CONFIG
# =============================================================================

@dataclass(frozen=True)
class AppConfig:
    query_string: Optional[str]
    page_size: Optional[int]
    inteiro_teor: bool
    headed_mode: bool
    output_dir: Optional[str]
    url_scheme: Optional[str]
    url_netloc: Optional[str]
    url_path: Optional[str]

    def pretty_print(self) -> None:
        print("\n= CONFIGURAÇÕES (VARIÁVEIS) =")
        print(f"query_string : {self.query_string!r}")
        print(f"page_size    : {self.page_size!r}")
        print(f"inteiro_teor : {self.inteiro_teor!r}")
        print(f"headed_mode  : {self.headed_mode!r}")
        print(f"output_dir   : {self.output_dir!r}")
        print(f"url_scheme   : {self.url_scheme!r}")
        print(f"url_netloc   : {self.url_netloc!r}")
        print(f"url_path     : {self.url_path!r}")
        print("= FIM =\n")


# =============================================================================
# 4) PARSERS UTILITÁRIOS (tolerantes)
# =============================================================================

def _as_str(value: Any) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s != "" else None


def _as_bool(value: Any, default: bool = False) -> bool:
    """
    Aceita: true/false, 1/0, yes/no, y/n, on/off (case-insensitive).
    """
    s = _as_str(value)
    if s is None:
        return default
    s = s.lower()
    true_set = {"true", "1", "yes", "y", "on"}
    false_set = {"false", "0", "no", "n", "off"}
    if s in true_set:
        return True
    if s in false_set:
        return False
    return default


def _as_int(value: Any) -> Optional[int]:
    """
    Converte com tolerância:
    - "30" -> 30
    - "30.0" -> 30
    - ""/None -> None
    """
    s = _as_str(value)
    if s is None:
        return None
    try:
        # permite "30.0"
        f = float(s.replace(",", "."))
        if f.is_integer():
            return int(f)
        return int(f)  # fallback: trunca (ou troque para levantar erro)
    except ValueError:
        return None


# =============================================================================
# 5) GOOGLE SHEETS: AUTH + READ
# =============================================================================

def build_gspread_client(service_account_file: Path) -> gspread.Client:
    if not service_account_file.exists():
        log("ERROR", f"Credenciais não encontradas: {service_account_file.resolve()}")
        raise FileNotFoundError("Arquivo de credenciais não localizado")

    scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
    creds = Credentials.from_service_account_file(str(service_account_file), scopes=scopes)
    log("INFO", "Credenciais carregadas")
    return gspread.authorize(creds)


def read_worksheet_as_df(
    gc: gspread.Client,
    spreadsheet_url: str,
    worksheet_name: str,
) -> pd.DataFrame:
    sh = gc.open_by_url(spreadsheet_url)
    ws = sh.worksheet(worksheet_name)
    log("INFO", f"Abrindo aba: {worksheet_name}")

    records = ws.get_all_records()
    df = pd.DataFrame(records)
    log("INFO", f"Registros lidos: {len(df)}")
    return df


# =============================================================================
# 6) NORMALIZAÇÃO / VALIDAÇÃO / FILTRO
# =============================================================================

def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip() for c in df.columns]
    return df


def resolve_status_column(df: pd.DataFrame, aliases: Sequence[str]) -> pd.DataFrame:
    df = df.copy()
    status_col = next((c for c in aliases if c in df.columns), None)
    if not status_col:
        log("ERROR", f"Coluna de status ausente (esperado: {list(aliases)})")
        raise ValueError("Coluna de status inexistente")

    if status_col != "status":
        df = df.rename(columns={status_col: "status"})
        log("WARN", f"Renomeado '{status_col}' -> 'status'")

    return df


def validate_required_columns(df: pd.DataFrame, required: Sequence[str]) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        log("ERROR", f"Colunas obrigatórias ausentes: {missing}")
        raise ValueError("Estrutura da planilha incompatível")


def filter_and_select_configs(
    df: pd.DataFrame,
    filter_statuses: Set[str],
    required_columns: Sequence[str],
    sort_for_dedup: Sequence[str],
) -> pd.DataFrame:
    df = df.copy()

    df["status"] = df["status"].astype(str).str.strip().str.lower()
    filter_norm = {str(s).strip().lower() for s in filter_statuses}

    out = df[df["status"].isin(filter_norm)][list(required_columns)].copy()
    log("INFO", f"Linhas com status em {sorted(filter_norm)}: {len(out)}")

    if out.empty:
        return out

    # Torna o "último vence" determinístico caso haja duplicidade de config_name
    for col in sort_for_dedup:
        if col not in out.columns:
            # se o sort não existir, não quebra
            continue
    sort_cols = [c for c in sort_for_dedup if c in out.columns]
    if sort_cols:
        out = out.sort_values(by=sort_cols, kind="stable")

    return out.reset_index(drop=True)


def df_to_config_dict(df: pd.DataFrame) -> Dict[str, Any]:
    if df.empty:
        return {}
    # Se config_name duplicar, o último (após sort) vence
    return df.set_index("config_name")["value"].to_dict()


# =============================================================================
# 7) MAPEAMENTO FINAL (dict -> AppConfig)
# =============================================================================

def build_app_config(configs: Dict[str, Any]) -> AppConfig:
    return AppConfig(
        query_string=_as_str(configs.get("query_string")),
        page_size=_as_int(configs.get("page_size")),
        inteiro_teor=_as_bool(configs.get("inteiro_teor"), default=False),
        headed_mode=_as_bool(configs.get("headed_mode"), default=False),
        output_dir=_as_str(configs.get("output_dir")),
        url_scheme=_as_str(configs.get("url_scheme")),
        url_netloc=_as_str(configs.get("url_netloc")),
        url_path=_as_str(configs.get("url_path")),
    )


# =============================================================================
# 8) ORQUESTRAÇÃO
# =============================================================================

def load_configs() -> AppConfig:
    log("INFO", "Iniciando (auth -> consulta -> normalize -> filter -> parse)")
    gc = build_gspread_client(SERVICE_ACCOUNT_FILE)

    try:
        df = read_worksheet_as_df(gc, SPREADSHEET_URL, WORKSHEET_NAME)
    except Exception as e:
        # Fallback seguro quando credenciais estão inválidas/expiradas
        log("ERROR", f"Falha ao ler Google Sheets: {e}")
        return build_app_config({})
    if df.empty:
        log("WARN", "Sem dados na planilha")
        return build_app_config({})

    df = normalize_columns(df)
    log("INFO", f"Colunas: {list(df.columns)}")

    df = resolve_status_column(df, STATUS_ALIASES)
    validate_required_columns(df, REQUIRED_COLUMNS)

    filtered = filter_and_select_configs(
        df=df,
        filter_statuses=FILTER_STATUSES,
        required_columns=REQUIRED_COLUMNS,
        sort_for_dedup=SORT_FOR_DEDUP,
    )

    if filtered.empty:
        log("WARN", "Nenhuma linha encontrada para os status filtrados")
        return build_app_config({})

    configs = df_to_config_dict(filtered)
    return build_app_config(configs)


def main() -> None:
    cfg = load_configs()
    cfg.pretty_print()
    log("INFO", "Finalizado")


if __name__ == "__main__":
    main()
