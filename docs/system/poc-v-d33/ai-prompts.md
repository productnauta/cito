# Prompts e LLMs — CITO poc-v-d33

## Arquivos
- `config/prompts.yaml` contém templates e parâmetros.
- `config/providers.yaml` define providers e modelos.

## Prompts usados no pipeline
- `extract-legislation-from-md` → protocolo N/R.
- `extract-notes-from-md` → protocolo CITO-REF/1.
- `extract-doctrines-from-md` → protocolo CITO-DOCTRINE/1.
- `get_decision-details-stf` → JSON com `decisionDetails`.

## Etapas vinculadas
- Step06 usa `extract-legislation-from-md`.
- Step07 usa `extract-notes-from-md`.
- Step08 usa `extract-doctrines-from-md`.
- Step09 usa `get_decision-details-stf`.

## Formatos de saída
- Legislação: linhas `N|...` e `R|...`.
- Notas: linhas `H|`, `L|`, `I|`, `M|`.
- Doutrina: linhas `C|...`.
- Decisão: JSON válido, tolerante a reparo simples.
