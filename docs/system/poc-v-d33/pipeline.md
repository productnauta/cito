# Pipeline — CITO poc-v-d33

## Etapas
1. `step00-search-stf.py` — coleta HTML da busca e grava em `case_query`.
2. `step01-extract-cases.py` — extrai cards e grava em `case_data`.
3. `step02-get-case-html.py` — baixa HTML completo do processo.
4. `step03-clean-case-html.py` — limpa HTML e extrai conteúdo principal.
5. `step04-extract-sessions.py` — separa seções e gera Markdown.
6. `step05-extract-keywords-parties.py` — extrai partes e palavras‑chave.
7. `step06-extract-legislation-mistral.py` — extrai legislação via LLM.
8. `step07-extract-notes-mistral.py` — extrai notas via LLM.
9. `step08-doctrine-mistral.py` — extrai doutrina via LLM.
10. `step09-extract-decision-details-mistral.py` — extrai detalhes da decisão via LLM.

## Orquestração
- `step00-run-pipeline-02-09.py` executa steps 02–09 e respeita `config/pipeline.yaml`.
- `step00-run-pipeline-from-case-query.py` executa a partir de `case_query` sem depender de status.

## Status e métricas
- Cada etapa grava status em `processing.*` e atualiza `status.pipelineStatus`.
- Erros não interrompem por padrão se `stop_on_error` for `false`.

## Entradas e saídas
- Entrada principal: `identity.stfDecisionId`.
- Saídas: campos `caseContent.*`, `caseData.*`, `processing.*`.
